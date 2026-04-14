package com.mzinx.mongodb.changestream.model;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChangeStreamRegistry<T> {
	private String collectionName;
    private ChangeStream<T> changeStream;
    private Consumer<ChangeStreamDocument<T>> body;
    private CompletableFuture<Object> completableFuture;
    
    private int instanceIndex;
    private int instanceSize;
    private String leader;
    private List<String> instances;

    private Date earliestChangeAt;
    
    public void stop(){
		this.changeStream.setRunning(false);
		if (this.completableFuture != null)
			this.completableFuture.join();
    }
    
}
