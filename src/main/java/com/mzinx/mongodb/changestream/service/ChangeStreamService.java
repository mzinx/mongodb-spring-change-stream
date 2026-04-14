package com.mzinx.mongodb.changestream.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.resilience.annotation.Retryable;
import org.springframework.stereotype.Service;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mzinx.mongodb.aggregation.dao.PipelineRepository;
import com.mzinx.mongodb.aggregation.model.Aggregation;
import com.mzinx.mongodb.aggregation.model.PipelineTemplate;
import com.mzinx.mongodb.aggregation.service.AggregationService;
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.model.ChangeStream;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStream.ResumeStrategy;
import com.mzinx.mongodb.changestream.model.ChangeStreamRegistry;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class ChangeStreamService<T> {
	Logger logger = LoggerFactory.getLogger(getClass());
	private static final String INDEX_NAME = "ttl";
	private static final String CSID_FIELD = "_id.cs";
	private static final String HOST_FIELD = "_id.h";
	private static final String DATE_FIELD = "at";
	private static final String TOKEN_FIELD = "t";

	@Autowired
	private MongoTemplate mongoTemplate;
	@Autowired
	private Executor taskExecutor;

	@Autowired
	private Map<String, ChangeStreamRegistry<T>> changeStreams;

	@Autowired
	private AggregationService aggregationService;

	@Autowired
	private PipelineRepository pipelineRepository;

	@Autowired
	private Set<String> instances;

	@Autowired
	private ChangeStreamProperties changeStreamProperties;

	private static final Set<Consumer<ChangeStreamDocument<?>>> listeners = new HashSet<>();

	private static final String RESUME_TOKEN_PIPELINE_NAME = "getResumeToken";
	private PipelineTemplate pipelineTemplate;

	// *****TODO: TEST AUTO SCALING, START/STOP node, START/STOP all*****

	@PostConstruct
	private void init() {
		mongoTemplate.getCollection(changeStreamProperties.getResumeTokenCollection())
				.createIndex(Indexes.descending(DATE_FIELD),
						new IndexOptions()
								.expireAfter(changeStreamProperties.getTokenMaxLifeTime(), TimeUnit.MILLISECONDS)
								.name(INDEX_NAME));

		this.pipelineTemplate = this.pipelineRepository.findByName(RESUME_TOKEN_PIPELINE_NAME);
		if (this.pipelineTemplate == null || this.pipelineTemplate.getContent() == null) {
			PipelineTemplate p = new PipelineTemplate();
			p.setName(RESUME_TOKEN_PIPELINE_NAME);
			List<Bson> ps = List.of(
					Aggregates.match(
							Filters.expr(
									Filters.and(
											new Document("$eq", List.of("$_id.cs",
													new Document("_ph", "csId"))),
											new Document("$gte", List.of("$at",
													new Document("$ifNull",
															List.of(new Document("_ph", "earliest"), "$at"))))))),
					Aggregates.group("$_id.h", Accumulators.first("d", "$$ROOT")),
					Aggregates.sort(Sorts.ascending("d.at")),
					Aggregates.limit(1),
					Aggregates.project(Projections.fields(
							Projections.computed("_id", "$d._id"),
							Projections.computed("h", "$d.h"),
							Projections.computed("at", "$d.at"),
							Projections.computed("t", "$d.t"))));
			p.setContent(ps.stream().map(a -> a.toBsonDocument()).collect(Collectors.toList()));
			p.setAggs(p.getContent().stream().map(d -> new Document(d)).collect(Collectors.toList()));
			this.pipelineTemplate = this.pipelineRepository.save(p);
		}

		this.subscribe(event -> {
			// watch instances collection change
			if (event.getNamespace().getCollectionName().equals(changeStreamProperties.getInstanceCollection())) {
				try {
					String instance = event.getDocumentKey().getString("_id").getValue();
					for (String csId : changeStreams.keySet()) {
						ChangeStreamRegistry<T> reg = changeStreams.get(csId);

						switch (event.getOperationType()) {
							case INSERT:
								this.shouldRun(reg, Mode.AUTO_SCALE == reg.getChangeStream().getMode());
								break;
							case UPDATE:
								this.shouldRun(reg);
								break;
							case DELETE:
								this.runForSubstitute(csId, instance,
										changeStreamProperties.getHostname());
								break;
							default:
								break;

						}
					}
					// if (OperationType.DELETE == event.getOperationType()) {
					// }
				} catch (RuntimeException e) {
					logger.error("Unexpected error while processing node changes:", e);
				}
			}
		});
	}

	@PreDestroy
	private void destroy() {
		for (String csId : changeStreams.keySet()) {
			this.stop(changeStreams.get(csId), false);
			this.changeStreams.remove(csId);
		}
		this.clear();
	}

	private void runForLeader(ChangeStreamRegistry<T> reg) {
		Document doc = mongoTemplate.getCollection(changeStreamProperties.getChangeStreamCollection()).findOneAndUpdate(
				Filters.eq("_id", reg.getChangeStream().getId()), List.of(
						new Document("$set", new Document()
								.append("_id", reg.getChangeStream().getId())
								.append("l",
										new Document("$ifNull",
												List.of("$l", changeStreamProperties.getHostname())))
								.append("at", new Date())
								.append("i", new Document(
										"$setUnion", List.of(
												new Document("$ifNull", List.of("$i", List.of())),
												List.of(changeStreamProperties.getHostname())))))),
				new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
		reg.setLeader(doc.getString("l"));
		reg.setInstances(doc.getList("i", String.class));
		this.changeStreams.put(reg.getChangeStream().getId(), reg);
	}

	private void runForSubstitute(String csId, String deadHost) {
		this.runForSubstitute(csId, deadHost, null);
	}

	private void runForSubstitute(String csId, String deadHost, String replacement) {
		Document doc = null;
		if (deadHost == null && replacement == null) {
			doc = mongoTemplate
					.getCollection(changeStreamProperties.getChangeStreamCollection()).findOneAndUpdate(
							Filters.eq("_id", csId), List.of(
									new Document("$set", new Document()
											.append("_id", csId)
											.append("l", null)
											.append("at", new Date())
											.append("i", List.of()))),
							new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
		} else {
			doc = mongoTemplate
					.getCollection(changeStreamProperties.getChangeStreamCollection()).findOneAndUpdate(
							Filters.eq("_id", csId), List.of(
									new Document("$set", new Document()
											.append("_id", csId)
											.append("l", new Document("$cond",
													List.of(new Document("$eq",
															List.of(new Document("$ifNull", List.of("$l", deadHost)),
																	deadHost)),
															replacement,
															"$l")))
											.append("at", new Date())
											.append("i", new Document(
													"$filter", new Document()
															.append("input", "$i")
															.append("as", "item")
															.append("cond", new Document("$ne",
																	List.of("$$item", deadHost))))))),
							new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
		}
		if (this.changeStreams.containsKey(csId)) {
			this.changeStreams.get(csId).setLeader(doc.getString("l"));
			this.changeStreams.get(csId).setInstances(doc.getList("i", String.class));
		}
	}

	public void start(ChangeStreamRegistry<T> reg) {
		this.logger.info("Start running change stream:" + reg.getChangeStream().getId());
		this.runForLeader(reg);
		this.prepareResumeToken(reg);
		this.shouldRun(reg);
	}

	private void prepareResumeToken(ChangeStreamRegistry<T> reg) {
		ChangeStream<T> cs = reg.getChangeStream();
		this.logger.info("ResumeStrategy:" + cs.getResumeStrategy());
		if (ResumeStrategy.NONE != cs.getResumeStrategy()) {
			Aggregation<Document> agg = Aggregation.of(changeStreamProperties.getResumeTokenCollection(),
					this.pipelineTemplate.getContent());
			Map<String, Object> variables = new HashMap<>();
			variables.put("csId", cs.getId());
			variables.put("earliest", reg.getEarliestChangeAt());
			List<Document> l = aggregationService.execute(agg, variables);
			if (l.size() > 0) {
				this.logger.info("Resume change stream " + cs.getId() + " from: " + l);
				cs.resumeAfter(l.get(0).getString(TOKEN_FIELD));
			}
			reg.setEarliestChangeAt(null);
		}
	}

	public void restart(ChangeStreamRegistry<T> reg) {
		reg.stop();
		this.prepareResumeToken(reg);
		if (Mode.AUTO_SCALE == reg.getChangeStream().getMode()) {
			this.scale(reg);
		} else {
			this.run(reg);
		}
	}

	public void shouldRun(ChangeStreamRegistry<T> reg) {
		this.shouldRun(reg, false);
	}

	@Retryable(includes = { MongoException.class }, maxRetries = 10, delay = 5000, multiplier = 2)
	public void shouldRun(ChangeStreamRegistry<T> reg, boolean forceRestart) {
		this.logger.debug("csid: " + reg.getChangeStream().getId() + ", leader: " + reg.getLeader() + ", instances: "
				+ reg.getInstances());
		boolean shouldRun = reg.getInstances().contains(changeStreamProperties.getHostname());

		this.logger.debug(reg.getChangeStream().getId() + " shouldRun:" + shouldRun + " isRunning:"
				+ reg.getChangeStream().isRunning() + " Mode:" + reg.getChangeStream().getMode());
		switch (reg.getChangeStream().getMode()) {
			case BOARDCAST:
				// Boardcast mode
				// -First start
				// --run for leader+register host
				// --get resume token
				// --start
				// -New member join (new)
				// --run for leader+register host
				// --get resume token
				// --start
				// -New member join (old)
				// --Do nothing
				// -Member die
				// --Do nothing
				// -Leader die
				// --Do nothing
				// -Stop
				// --deregister
				// --stop
				// -Start
				// --run for leader+register host
				// --get resume token
				// --start
				if (!reg.getChangeStream().isRunning()) {
					run(reg);
				} else if (!shouldRun) {
					reg.stop();
				} else {
					this.logger.debug("do nothing");
				}
				break;
			case AUTO_RECOVER:
				// Auto recovery mode
				// -First start
				// --run for leader+register host
				// --get resume token
				// --start
				// -New member join (new)
				// --run for leader+register host
				// --standby
				// -New member join (old)
				// --Do nothing
				// -Member die
				// --Do nothing
				// -Leader die
				// --run for leader
				// --get resume token
				// --restart
				// -Stop
				// --deregister
				// --stop
				// -Start
				// --run for leader+register host
				// --get resume token
				// --start
				if (reg.getLeader() == null) {
					this.logger.info("leader stepped down, try to take over.");
					this.start(reg);
				} else if (this.changeStreamProperties.getHostname().equals(reg.getLeader())) {
					if (!reg.getChangeStream().isRunning())
						this.run(reg);
				} else {
					if (reg.getChangeStream().isRunning())
						reg.stop();
				}
				break;
			case AUTO_SCALE:
				// Auto scale mode
				// First start
				// New member join
				// Member die
				// Leader die
				// Stop
				// Start
				if (shouldRun && !reg.getChangeStream().isRunning()) {
					this.logger
							.info("I should run the change stream " + reg.getChangeStream().getId() + ", start now.");
					this.scale(reg);
				} else if (!shouldRun && reg.getChangeStream().isRunning()) {
					this.logger
							.info("I shouldn't run the change stream " + reg.getChangeStream().getId() + ", stop now.");
					reg.stop();
				} else if (forceRestart) {
					this.restart(reg);
				}
				break;
			default:
				break;

		}
		// Deregister dead member??
	}

	public void scale(ChangeStreamRegistry<T> reg) {
		ChangeStream<T> cs = reg.getChangeStream();
		int index = new ArrayList<String>(this.instances).indexOf(this.changeStreamProperties.getHostname());
		reg.setInstanceSize(this.instances.size());
		reg.setInstanceIndex(index);
		if (this.instances.size() > 0 && index >= 0) {
			if (cs.isRunning()) {
				reg.stop();
			}
			this.logger.info("change stream " + (index + 1) + " of " + this.instances.size()
					+ " start running in "
					+ this.changeStreamProperties.getHostname());
			this.run(reg);
		} else {
			this.logger.info("Node " + this.changeStreamProperties.getHostname() + " is not ready to run change stream:"
					+ cs.getId());
		}
	}

	private void run(ChangeStreamRegistry<T> reg) {
		CompletableFuture<Object> completableFuture = CompletableFuture.supplyAsync(() -> {
			try {
				if (reg.getCollectionName() != null) {
					reg.getChangeStream().watch(mongoTemplate.getCollection(reg.getCollectionName()), reg,
							resumeToken -> {
								saveCheckpoint(reg.getChangeStream().getId(), resumeToken);
							});
				} else {
					reg.getChangeStream().watch(mongoTemplate.getDb(), reg, resumeToken -> {
						saveCheckpoint(reg.getChangeStream().getId(), resumeToken);
					});
				}
			} catch (MongoInterruptedException e) {
				this.logger.info("Change stream '" + reg.getChangeStream().getId() + "' Interrupted");
			} catch (RuntimeException e) {
				if (e instanceof MongoCommandException && ((MongoCommandException) e).getCode() == 286) {
					this.logger.warn("Change stream '" + reg.getChangeStream().getId()
							+ "' history lost, token: '"+ reg.getChangeStream().getResumeToken() +"', restart without resume token");
							reg.getChangeStream().setResumeToken(null);
					mongoTemplate.getCollection(changeStreamProperties.getResumeTokenCollection()).deleteOne(
							Filters.and(Filters.eq(CSID_FIELD, reg.getChangeStream().getId()),
									Filters.eq(HOST_FIELD, changeStreamProperties.getHostname())));
				} else {
					this.logger.error(
							"Stopping change stream '" + reg.getChangeStream().getId() + "' due to unexpected error:",
							e);
				}
				reg.stop();
				// Recover for unexpected exception
				this.start(reg);
			}
			return null;
		}, taskExecutor);
		reg.setCompletableFuture(completableFuture);
		this.logger.info("Change stream " + reg.getChangeStream().getId() + " started");
	}

	public void stop(ChangeStreamRegistry<T> reg, boolean stopAll) {
		this.logger.info("Stop change stream");
		reg.stop();
		runForSubstitute(reg.getChangeStream().getId(), stopAll ? null : changeStreamProperties.getHostname());
	}

	@Retryable(includes = { MongoException.class }, maxRetries = 10, delay = 1000, multiplier = 2)
	private void saveCheckpoint(String csId, BsonString token) {
		logger.info("save checkpoint");
		mongoTemplate.getCollection(changeStreamProperties.getResumeTokenCollection()).updateOne(
				Filters.and(Filters.eq(CSID_FIELD, csId), Filters.eq(HOST_FIELD, changeStreamProperties.getHostname())),
				Updates.combine(Updates.set(CSID_FIELD, csId),
						Updates.combine(Updates.set(HOST_FIELD, changeStreamProperties.getHostname()),
								Updates.set(DATE_FIELD, new Date()),
								Updates.set(TOKEN_FIELD, token))),
				new UpdateOptions().upsert(true));
	}

	public void publish(ChangeStreamDocument<T> event) {
		logger.debug("new event:" + event);
		listeners.forEach(l -> {
			CompletableFuture.supplyAsync(() -> {
				try {
					l.accept(event);
				} catch (RuntimeException e) {
					logger.error("Unexpected error publishing event:", e);
				}
				return null;
			}, taskExecutor);
		});
	}

	public void subscribe(Consumer<ChangeStreamDocument<?>> listener) {
		logger.info("new subscription:" + listeners.add(listener));
	}

	public void unsubscribe(Consumer<ChangeStreamDocument<T>> listener) {
		logger.info("remove subscription:" + listeners.remove(listener));
	}

	public void clear() {
		logger.info("Clear all subscription");
		listeners.clear();
	}
}
