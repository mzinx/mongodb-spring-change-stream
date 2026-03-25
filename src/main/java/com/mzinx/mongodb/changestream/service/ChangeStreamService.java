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
import com.mongodb.client.model.changestream.OperationType;
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
					if (OperationType.INSERT == event.getOperationType()
							|| OperationType.DELETE == event.getOperationType()) {
						logger.info("change streams running in this node " + changeStreams.keySet());
					}
					String instance = event.getDocumentKey().getString("_id").getValue();
					Document document = (Document) event.getFullDocument();

					for (String csId : changeStreams.keySet()) {
						ChangeStreamRegistry<T> reg = changeStreams.get(csId);
						ChangeStream<T> cs = reg.getChangeStream();
						switch (event.getOperationType()) {
							case INSERT:
								if (Mode.AUTO_SCALE == cs.getMode()) {
									this._stop(reg);
									run(reg, event.getFullDocument() != null
											? document.getDate("at")
											: null);
								}
								break;
							case UPDATE:
								if (Mode.AUTO_SCALE == cs.getMode()) {
									if (!cs.isRunning()) {
										logger.info(
												cs.getId() + " is not yet running in "
														+ changeStreamProperties.getHostname() + " start it now.");
										scale(reg);
									}
								}
								break;
							case DELETE:
								Document doc = this.runForSubstitute(csId, instance, changeStreamProperties.getHostname());
								
								if (Mode.AUTO_SCALE == cs.getMode()) {
									this._stop(reg);
									run(reg, ((Document) event.getFullDocumentBeforeChange()).getDate("at"));
								}
								break;
							default:
								break;
						}
					}
				} catch (RuntimeException e) {
					logger.error("Unexpected error while processing node changes:", e);
				}
			}
		});
	}

	@PreDestroy
	private void destroy() {
		this.clear();
		for (String csId : changeStreams.keySet()) {
			this.stop(changeStreams.get(csId), false);
			changeStreams.remove(csId);
		}
	}

	private Document runForLeader(ChangeStreamRegistry<T> reg) {
		Document doc = mongoTemplate.getCollection(changeStreamProperties.getChangeStreamCollection()).findOneAndUpdate(
				Filters.eq("_id", reg.getChangeStream().getId()), List.of(
						new Document("$set", new Document()
								.append("_id", reg.getChangeStream().getId())
								.append("m",
										new Document("$ifNull",
												List.of("$m", changeStreamProperties.getHostname())))
								.append("at", new Date())
								.append("i", new Document(
										"$setUnion", List.of(
												new Document("$ifNull", List.of("$i", List.of())),
												List.of(changeStreamProperties.getHostname())))))),
				new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
		reg.setInstances(doc.getList("i", String.class));
		changeStreams.put(reg.getChangeStream().getId(), reg);
		return doc;
	}

	private Document runForSubstitute(String csId, String deadHost) {
		return runForSubstitute(csId, deadHost, null);
	}
	private Document runForSubstitute(String csId, String deadHost, String replacement) {
		Document doc = mongoTemplate
				.getCollection(changeStreamProperties.getChangeStreamCollection()).findOneAndUpdate(
						Filters.eq("_id", csId), List.of(
								new Document("$set", new Document()
										.append("_id", csId)
										.append("m", new Document("$cond",
												List.of(new Document("$eq", List.of(new Document("$ifNull", List.of("$m", deadHost)), deadHost)),
														replacement,
														"$m")))
										.append("at", new Date())
										.append("i", new Document(
												"$filter", new Document()
														.append("input", "$i")
														.append("as", "item")
														.append("cond", new Document("$ne",
																List.of("$$item", deadHost))))))),
						new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
		if (changeStreams.containsKey(csId))
			changeStreams.get(csId).setInstances(doc.getList("i", String.class));
		return doc;
	}

	public void run(ChangeStreamRegistry<T> reg) {
		this.run(reg, null);
	}

	@Retryable(includes = { MongoException.class }, maxRetries = 10, delay = 5000, multiplier = 2)
	public void run(ChangeStreamRegistry<T> reg, Date earliest) {
		logger.info("Start running change stream");
		Document doc = runForLeader(reg);
		ChangeStream<T> cs = reg.getChangeStream();

		logger.info("ResumeStrategy:" + cs.getResumeStrategy());
		if (ResumeStrategy.NONE != cs.getResumeStrategy()) {
			Aggregation<Document> agg = Aggregation.of(changeStreamProperties.getResumeTokenCollection(),
					this.pipelineTemplate.getContent());
			Map<String, Object> variables = new HashMap<>();
			variables.put("csId", cs.getId());
			variables.put("earliest", earliest);
			List<Document> l = aggregationService.execute(agg, variables);
			if (l.size() > 0) {
				cs.resumeAfter(l.get(0).getString(TOKEN_FIELD));
			}
		}

		logger.info("Mode:" + cs.getMode());
		if (Mode.AUTO_RECOVER == cs.getMode()) {
			if (changeStreamProperties.getHostname().equals(doc.getString("m"))) {
				if (cs.isRunning()) {
					logger.info("Change stream already running in this node");
				} else {
					start(reg);
				}
			} else {
				logger.info("Change stream already running in other node:" + doc);
			}
		} else if (Mode.AUTO_SCALE == cs.getMode()) {
			scale(reg);
		} else {
			start(reg);
		}
	}

	private void start(ChangeStreamRegistry<T> reg) {
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
			} catch (MongoInterruptedException e){				
				logger.info("Change stream '" + reg.getChangeStream().getId() + "' Interrupted");
			} catch (RuntimeException e) {
				logger.error("Stopping change stream '" + reg.getChangeStream().getId() + "' due to unexpected error:",
						e);
				this._stop(reg);
				// Recover for unexpected exception
				this.run(reg);
			}
			return null;
		}, taskExecutor);
		reg.setCompletableFuture(completableFuture);
		logger.info("Change stream " + reg.getChangeStream().getId() + " started");
	}

	public void stop(ChangeStreamRegistry<T> reg, boolean stopAll) {
		logger.info("Stop change stream");
		this._stop(reg);
		runForSubstitute(reg.getChangeStream().getId(), stopAll?null:changeStreamProperties.getHostname());
	}

	public void _stop(ChangeStreamRegistry<T> reg) {
		reg.getChangeStream().setRunning(false);
		if (reg.getCompletableFuture() != null)
			reg.getCompletableFuture().join();
	}

	private void scale(ChangeStreamRegistry<T> reg) {
		ChangeStream<T> cs = reg.getChangeStream();
		int index = new ArrayList<String>(this.instances).indexOf(changeStreamProperties.getHostname());
		reg.setInstanceSize(this.instances.size());
		reg.setInstanceIndex(index);
		if (this.instances.size() > 0 && index >= 0) {
			if (cs.isRunning()) {
				this._stop(reg);
			}
			logger.info("change stream " + (index + 1) + " of " + this.instances.size()
					+ " start running in "
					+ changeStreamProperties.getHostname());
			start(reg);
		} else {
			logger.info("Node " + changeStreamProperties.getHostname() + " is not ready to run change stream:"
					+ cs.getId());
		}
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
