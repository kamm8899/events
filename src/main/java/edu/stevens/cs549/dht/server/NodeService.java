package edu.stevens.cs549.dht.server;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
import edu.stevens.cs549.dht.activity.IDhtNode;
import edu.stevens.cs549.dht.activity.IDhtService;
import edu.stevens.cs549.dht.events.EventProducer;
import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceImplBase;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Additional resource logic.  The Web resource operations call
 * into wrapper operations here.  The main thing these operations do
 * is to call into the DHT service object, and wrap internal exceptions
 * as HTTP response codes (throwing WebApplicationException where necessary).
 * 
 * This should be merged into NodeResource, then that would be the only
 * place in the app where server-side is dependent on JAX-RS.
 * Client dependencies are in WebClient.
 * 
 * The activity (business) logic is in the dht object, which exposes
 * the IDHTResource interface to the Web service.
 */

public class NodeService extends DhtServiceImplBase {
	
	private static final String TAG = NodeService.class.getCanonicalName();
	
	private static Logger logger = Logger.getLogger(TAG);

	/**
	 * Each service request is processed by a distinct service object.
	 *
	 * Shared state is in the state object; we use the singleton pattern to make sure it is shared.
	 */
	private IDhtService getDht() {
		return Dht.getDht();
	}
	
	// TODO: add the missing operations
	@Override
	public void getPred(Empty empty, StreamObserver<OptNodeInfo> responseObserver) {
		Log.weblog(TAG, "getPred()");
		responseObserver.onNext(getDht().getPred());
		responseObserver.onCompleted();
	}

	@Override
	public void getSucc(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getSucc()");
		responseObserver.onNext(getDht().getSucc());
		responseObserver.onCompleted();
	}

	@Override
	public void findSuccessor(Id id, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "findSuccessor(" + id.getId() + ")");
		try {
			NodeInfo result = getDht().findSuccessor(id.getId());
			responseObserver.onNext(result);
			responseObserver.onCompleted();
		} catch (Failed e) {
			error("findSuccessor failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void notify(NodeBindings request, StreamObserver<OptNodeBindings> responseObserver) {
		Log.weblog(TAG, "notify(" + request.getInfo().getId() + ")");
		OptNodeBindings result = getDht().notify(request);
		responseObserver.onNext(result);
		responseObserver.onCompleted();
	}

	@Override
	public void addBinding(Binding request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "addBinding()");
		try {
			getDht().add(request.getKey(), request.getValue());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		} catch (Invalid e) {
			error("addBinding failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getBindings(Key request, StreamObserver<Bindings> responseObserver) {
		Log.weblog(TAG, "getBindings(" + request.getKey() + ")");
		try {
			String[] values = getDht().get(request.getKey());
			Bindings.Builder builder = Bindings.newBuilder()
					.setKey(request.getKey());
			if (values != null) {
				for (String value : values) {
					builder.addValue(value);
				}
			}
			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		} catch (Invalid e) {
			error("getBindings failed", e);
			responseObserver.onError(e);
		}
	}
	@Override
	public void deleteBinding(Binding request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "deleteBinding()");
		try {
			getDht().delete(request.getKey(), request.getValue());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		} catch (Invalid e) {
			error("deleteBinding failed", e);
			responseObserver.onError(e);
		}
	}
	private void error(String mesg, Exception e) {
		logger.log(Level.SEVERE, mesg, e);
	}

	@Override
	public void getNodeInfo(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getNodeInfo()");
		responseObserver.onNext(getDht().getNodeInfo());
		responseObserver.onCompleted();
	}

	@Override
	public void listenOn(Subscription request, StreamObserver<Event> responseObserver) {
		Log.weblog(TAG, "listenOn(" + request.getId() + "," + request.getKey() + ")");
		try {
			// Create an event producer that will send events to the client
			EventProducer producer = EventProducer.create(responseObserver);
			// Register the producer as a listener for the specified key
			getDht().listenOn(request.getId(), request.getKey(), producer);
		} catch (Exception e) {
			error("listenOn failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void listenOff(Subscription request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "listenOff(" + request.getId() + "," + request.getKey() + ")");
		try {
			// Remove the listener for the specified key
			getDht().listenOff(request.getId(), request.getKey());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		} catch (Exception e) {
			error("listenOff failed", e);
			responseObserver.onError(e);
		}
	}
	@Override
	public void closestPrecedingFinger(Id request, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "closestPrecedingFinger(" + request.getId() + ")");
		NodeInfo result = getDht().closestPrecedingFinger(request.getId());
		responseObserver.onNext(result);
		responseObserver.onCompleted();
	}


}