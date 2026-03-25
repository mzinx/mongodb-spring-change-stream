package com.mzinx.mongodb.changestream.bootstrap;

import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.model.ChangeStream;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStreamRegistry;
import com.mzinx.mongodb.changestream.service.ChangeStreamService;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Component
public class ChangeStreamRunner {
    Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ChangeStreamService<Document> changeStreamService;

	@Autowired
	private ChangeStreamProperties changeStreamProperties;
    ChangeStream<Document> cs;

    @PostConstruct
    private void watch() {
        cs = ChangeStream.of("change-stream", Mode.BOARDCAST,
                List.of(Aggregates.match(
                        Filters.in("operationType", List.of("insert", "update", "delete")))));
        this.changeStreamService.run(ChangeStreamRegistry.<Document>builder()
                .collectionName(changeStreamProperties.getChangeStreamCollection()).body(e -> {
                    this.logger.info("change stream changes: " + e);
                    switch (e.getOperationType()) {
                        case INSERT:
                        case UPDATE:

                            //fullDocument
                            
                            // TODO: If leader is null & i is empty, stop the change stream
                            //updatedFields={"m": null, "at": {"$date": "2026-03-25T18:04:16.179Z"}, "i": []}


                            // TODO: If leader or i changed, start/stop
                            //updatedFields={"m": "host1", "at": {"$date": "2026-03-25T18:08:57.29Z"}, "i": ["host1"]}
                            /*if (Mode.AUTO_RECOVER == cs.getMode()) {
									String leader = doc.getString("m");
									if (changeStreamProperties.getHostname().equals(leader)) {
										if (cs.isRunning()) {
											if (!changeStreamProperties.getHostname().equals(instance))
												logger.info(
														instance + " is dead, I'm still running change stream:"
																+ cs.getId());
											else {
												logger.info("Change stream " + cs.getId()
														+ " already running in this node");

											}
										} else {
											logger.info(
													instance + " is dead, " + changeStreamProperties.getHostname()
															+ " try to take over change stream:"
															+ cs.getId());
											this._stop(reg);
											run(reg);
										}
									} else {
										logger.info("Change stream already running in other node:" + doc);
										if (cs.isRunning()) {
											logger.info(
													"Change stream running in this node should stop.");
											this._stop(reg);
										}
									}
								}*/

                            break;
                        case DELETE:
                            break;
                        default:
                            break;
                    }
                }).changeStream(cs).build());
    }

    @PreDestroy
    private void clear() {
        if (cs != null)
            cs.setRunning(false);
    }

}
