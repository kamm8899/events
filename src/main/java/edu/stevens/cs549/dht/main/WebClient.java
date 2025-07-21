package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.events.EventConsumer;
import edu.stevens.cs549.dht.events.IEventListener;
import edu.stevens.cs549.dht.rpc.Binding;
import edu.stevens.cs549.dht.rpc.Bindings;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceBlockingStub;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceStub;
import edu.stevens.cs549.dht.rpc.Id;
import edu.stevens.cs549.dht.rpc.Key;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.rpc.OptNodeBindings;
import edu.stevens.cs549.dht.rpc.OptNodeInfo;
import edu.stevens.cs549.dht.rpc.Subscription;
import edu.stevens.cs549.dht.state.IChannels;
import edu.stevens.cs549.dht.state.IState;
import io.grpc.Channel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebClient {
	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private IChannels channels;

	private WebClient(IChannels channels) {
		this.channels = channels;
	}

	public static WebClient getInstance(IState state) {
		return new WebClient(state.getChannels());
	}

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}

	/*
	 * Get a blocking stub (channels and stubs are cached for reuse).
	 */
	private DhtServiceBlockingStub getStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newBlockingStub(channel);
	}

	private DhtServiceBlockingStub getStub(NodeInfo target) throws DhtBase.Failed {
		return getStub(target.getHost(), target.getPort());
	}

	private DhtServiceStub getListenerStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newStub(channel);
	}

	private DhtServiceStub getListenerStub(NodeInfo target) throws DhtBase.Failed {
		return getListenerStub(target.getHost(), target.getPort());
	}


	/*
	 * TODO: Fill in missing operations.
	 */

	/*
	 * Get the predecessor pointer at a node.
	 */
	public OptNodeInfo getPred(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "getPred("+node.getId()+")");
		return getStub(node).getPred(Empty.getDefaultInstance());
	}

	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public OptNodeBindings notify(NodeInfo node, NodeBindings predDb) throws DhtBase.Failed {
		Log.weblog(TAG, "notify("+node.getId()+")");
		// TODO
		Log.weblog(TAG, "notify(" + node.getId() + ")");

		try {
			// Create a stub for the target node
			DhtServiceGrpc.DhtServiceBlockingStub stub = getStub(node);

			// Make the remote call to notify the target node
			OptNodeBindings response = stub.notify(predDb);

			// Check if the response contains bindings
			if (response == null || !response.hasNodeBindings()) {
				Log.weblog(TAG, "notify(" + node.getId() + ") was rejected or returned no bindings.");
				return null;
			}

			// Log success and return the bindings
			Log.weblog(TAG, "notify(" + node.getId() + ") accepted.");
			return response;

		} catch (Exception e) {
			error("Notify failed for node " + node.getId(), e);
			throw new DhtBase.Failed("Notify RPC failed: " + e.getMessage());
		}
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
	}
	public NodeInfo findSuccessor(NodeInfo node, Id id) throws DhtBase.Failed {
		Log.weblog(TAG, "findSuccessor(" + id.getId() + ") at node " + node.getId());
		try {
			return getStub(node).findSuccessor(id);
		} catch (Exception e) {
			error("findSuccessor RPC failed", e);
			throw new DhtBase.Failed("findSuccessor RPC failed");
		}
	}
	public NodeInfo closestPrecedingFinger(NodeInfo node, int id) throws DhtBase.Failed {
		Log.weblog(TAG, "closestPrecedingFinger(" + id + ") at node " + node.getId());
		try {
			Id request = Id.newBuilder().setId(id).build();
			return getStub(node).closestPrecedingFinger(request);
		} catch (Exception e) {
			error("closestPrecedingFinger RPC failed", e);
			throw new DhtBase.Failed("closestPrecedingFinger RPC failed");
		}
	}

	public NodeInfo getSucc(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "getSucc(" + node.getId() + ")");
		try {
			return getStub(node).getSucc(Empty.getDefaultInstance());
		} catch (Exception e) {
			error("getSucc RPC failed", e);
			throw new DhtBase.Failed("getSucc RPC failed");
		}
	}
	public void addBinding(NodeInfo node, Key key, Binding val) throws DhtBase.Failed {
		Log.weblog(TAG, "addBinding(" + key.getKey() + ", " + val.getValue() + ") at node " + node.getId());
		try {
			// Build a simple Binding message
			Binding binding = Binding.newBuilder()
					.setKey(key.getKey())
					.setValue(val.getValue())
					.build();

			getStub(node).addBinding(binding);  // ✅ now using correct type
		} catch (Exception e) {
			error("addBinding RPC failed", e);
			throw new DhtBase.Failed("addBinding RPC failed");
		}
	}
	public Bindings getBindings(NodeInfo node, Key key) throws DhtBase.Failed {
		Log.weblog(TAG, "getBindings(" + key.getKey() + ") at node " + node.getId());
		try {
			return getStub(node).getBindings(key);
		} catch (Exception e) {
			error("getBindings RPC failed", e);
			throw new DhtBase.Failed("getBindings RPC failed");
		}
	}

	public void deleteBinding(NodeInfo node, Binding b) throws DhtBase.Failed {
		Log.weblog(TAG, "deleteBinding(" + b.getKey() + ") at node " + node.getId());
		try {
			getStub(node).deleteBinding(b);
		} catch (Exception e) {
			error("deleteBinding RPC failed", e);
			throw new DhtBase.Failed("deleteBinding RPC failed: " + e.getMessage());
		}
	}


/*
	 * Listening for new bindings.
	 */
// TODO listen for updates for the key specified in the subscription
public void listenOn(NodeInfo node, Subscription subscription, IEventListener listener)
		throws DhtBase.Failed {
	info("listenOn(" + node.getId() + ")");
	try {
		DhtServiceStub asyncStub = getListenerStub(node);
		       asyncStub.listenOn(
					   subscription,
					   EventConsumer.create(subscription.getKey(), listener)
			   );
	} catch (Exception e) {
		error("listenOn RPC failed", e);
		throw new DhtBase.Failed("listenOn RPC failed: " + e.getMessage());
	}
}

	// TODO stop listening for updates on bindings to the key in the subscription
public void listenOff(NodeInfo node, Subscription subscription) throws DhtBase.Failed {
	info("listenOff(" + node.getId() + ")");
	try {
		DhtServiceBlockingStub stub = getStub(node);
		stub.listenOff(subscription);
	} catch (Exception e) {
		error("listenOff RPC failed", e);
		throw new DhtBase.Failed("listenOff RPC failed: " + e.getMessage());
	}
}

}
