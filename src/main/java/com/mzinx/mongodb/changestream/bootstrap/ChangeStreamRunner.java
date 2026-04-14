package com.mzinx.mongodb.changestream.bootstrap;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
    private ChangeStreamProperties changeStreamProperties;

    @Autowired
    private ChangeStreamService<Document> changeStreamService;

    @Autowired
    private Map<String, ChangeStreamRegistry<Document>> changeStreams;

    ChangeStream<Document> cs;

    @PostConstruct
    private void watch() {
        cs = ChangeStream.of("change-stream", Mode.BOARDCAST,
                List.of(Aggregates.match(
                        Filters.in("operationType", List.of("insert", "update", "delete")))));
        this.changeStreamService.start(ChangeStreamRegistry.<Document>builder()
                .collectionName(changeStreamProperties.getChangeStreamCollection()).body(e -> {
                    this.logger.info("change stream changes: " + e);
                    String csId = e.getDocumentKey().getString("_id").getValue();
                    ChangeStreamRegistry<Document> reg = changeStreams.get(csId);
                    String leader = null;
                    List<String> instances = new ArrayList<>();
                    Date changeAt = null;
                    boolean skip = false;
                    switch (e.getOperationType()) {
                        case INSERT:
                            leader = e.getFullDocument().getString("l");
                            instances.addAll(e.getFullDocument().getList("i", String.class));
                            changeAt = e.getFullDocument().getDate("at");
                            break;
                        case UPDATE:

                            if (e.getUpdateDescription().getUpdatedFields().containsKey("l"))
                                leader = e.getUpdateDescription().getUpdatedFields().getString("l").getValue();
                            if (e.getUpdateDescription().getUpdatedFields().containsKey("i"))
                                instances.addAll(e.getUpdateDescription().getUpdatedFields().getArray("i").stream()
                                        .map(i -> i.asString().getValue()).toList());
                            if (!e.getUpdateDescription().getUpdatedFields().containsKey("l")
                                    && !e.getUpdateDescription().getUpdatedFields().containsKey("i"))
                                skip = true;
                            changeAt = new Date(
                                    e.getUpdateDescription().getUpdatedFields().getDateTime("at").getValue());

                            break;
                        case DELETE:
                            //TODO
                            break;
                        default:
                            break;
                    }
                    if (!skip) {
                        if (reg.getEarliestChangeAt() == null || changeAt.before(reg.getEarliestChangeAt()))
                            reg.setEarliestChangeAt(changeAt);
                        if (changeStreamProperties.getHostname().equals(leader)) {
                            this.logger.info("I'm the leader, check change stream " + csId + " is running:"
                                    + reg.getChangeStream().isRunning());
                            if (Mode.AUTO_RECOVER == reg.getChangeStream().getMode()) {
                                if (!reg.getChangeStream().isRunning()) {
                                    this.logger.info("change stream " + csId + " is not running, start and take over.");
                                    this.changeStreamService.start(reg);
                                } else {
                                    this.logger.info("Still running the change stream:" + csId);
                                }
                            } else {
                                this.changeStreamService.shouldRun(reg, Mode.AUTO_SCALE == reg.getChangeStream().getMode());
                            }
                        } else {
                            this.logger.info("I'm not the leader");
                            this.changeStreamService.shouldRun(reg, Mode.AUTO_SCALE == reg.getChangeStream().getMode());
                        }
                    }
                }).changeStream(cs).build());
    }


    @PreDestroy
    private void clear() {
        if (cs != null)
            cs.setRunning(false);
    }

}
