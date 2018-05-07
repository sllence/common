package spark;

import com.alibaba.fastjson.JSON;
import org.apache.spark.streaming.scheduler.*;

/**
 * Created by zhouqi on 2018/5/3.
 */
public class MyStreamingListener implements StreamingListener {

	@Override
	public void onStreamingStarted(StreamingListenerStreamingStarted streamingStarted) {
	}

	@Override
	public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {

	}

	@Override
	public void onReceiverError(StreamingListenerReceiverError receiverError) {
	}

	@Override
	public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
	}

	@Override
	public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
	}

	@Override
	public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
	}

	@Override
	public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
	}

	@Override
	public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
	}

	@Override
	public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
		System.out.println(batchCompleted.batchInfo().toString());
		/*System.out.println(JSON.toJSONString(batchCompleted.batchInfo().batchTime().toString()));
		System.out.println(JSON.toJSONString(batchCompleted.batchInfo().numRecords()));
		System.out.println(JSON.toJSONString(batchCompleted.batchInfo().streamIdToInputInfo()));
		System.out.println(JSON.toJSONString(batchCompleted.batchInfo().outputOperationInfos()));
		System.out.println(JSON.toJSONString(batchCompleted.batchInfo().processingStartTime()));
		System.out.println(JSON.toJSONString(batchCompleted.batchInfo().processingEndTime()));
		System.out.println(JSON.toJSONString(batchCompleted.batchInfo().processingDelay()));
		System.out.println(JSON.toJSONString(batchCompleted.batchInfo().schedulingDelay()));*/
	}
}
