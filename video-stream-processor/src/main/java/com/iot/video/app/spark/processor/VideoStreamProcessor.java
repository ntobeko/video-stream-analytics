package com.iot.video.app.spark.processor;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.iot.video.app.spark.util.PropertyFileReader;
import com.iot.video.app.spark.util.VideoEventData;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoCapture;

import javax.swing.*;

/**
 * Class to consume incoming JSON messages from Kafka and process them using Spark Structured Streaming.
 *  
 * @author abaghel
 *
 */
public class VideoStreamProcessor {

	static {
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
	}


	private static final Logger logger = Logger.getLogger(VideoStreamProcessor.class);
	private static int cameraCount = 0;
	private static Map<String, JLabel> videoPanels = new HashMap<>();

	private static void initCameraWindows(VideoEventData videoEventData) {
		String cameraId = videoEventData.getCameraId();
		JLabel panel = videoPanels.get(cameraId);
		if (panel == null)
		{
			cameraCount++;
			JFrame jframe = new JFrame("Camera: " + cameraId);
			jframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
			panel = new JLabel();
			jframe.setLocation(cameraCount * 20, cameraCount * 20);
			jframe.setSize(320, 240);
			jframe.setContentPane(panel);
			jframe.setVisible(true);
			videoPanels.put(cameraId, panel);
		}

		updateView(videoEventData, null);
	}

	public static void updateView(VideoEventData videoEventData, Mat detectedMovementFrame) {
		JLabel panel = videoPanels.get(videoEventData.getCameraId());
		Mat frame = detectedMovementFrame;

		if (frame == null) {
			frame = VideoMotionDetector.getMat(videoEventData);
		}

		Mat resizedFrame = new Mat();
		Imgproc.resize(frame, resizedFrame, new Size(320, 240));
		ImageIcon image = new ImageIcon(Mat2BufferedImage(resizedFrame));
		panel.setIcon(image);

		if (detectedMovementFrame != null) {
			try {
				Thread.sleep(2000);
			} catch (Exception ignore) {}
		}
	}

	public static BufferedImage Mat2BufferedImage(Mat m) {
		//Method converts a Mat to a Buffered Image
		int type = BufferedImage.TYPE_BYTE_GRAY;
		if ( m.channels() > 1 ) {
			type = BufferedImage.TYPE_3BYTE_BGR;
		}
		int bufferSize = m.channels()*m.cols()*m.rows();
		byte [] b = new byte[bufferSize];
		m.get(0,0,b); // get all the pixels
		BufferedImage image = new BufferedImage(m.cols(),m.rows(), type);
		final byte[] targetPixels = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
		System.arraycopy(b, 0, targetPixels, 0, b.length);
		return image;
	}

	public static void main(String[] args) throws Exception {
	//Read properties
	Properties prop = PropertyFileReader.readPropertyFile();
	
	//SparkSesion
	SparkSession spark = SparkSession
		      .builder()
		      .appName("VideoStreamProcessor")
		      .master(prop.getProperty("spark.master.url"))
			  .config("spark.jars", "target/video-stream-processor-1.0.0.jar")
		      .getOrCreate();	
	
	//directory to save image files with motion detected
	final String processedImageDir = prop.getProperty("processed.output.dir");
	logger.warn("Output directory for saving processed images is set to "+processedImageDir+". This is configured in processed.output.dir key of property file.");
	
	//create schema for json message
	StructType schema =  DataTypes.createStructType(new StructField[] { 
			DataTypes.createStructField("cameraId", DataTypes.StringType, true),
			DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
			DataTypes.createStructField("rows", DataTypes.IntegerType, true),
			DataTypes.createStructField("cols", DataTypes.IntegerType, true),
			DataTypes.createStructField("type", DataTypes.IntegerType, true),
			DataTypes.createStructField("data", DataTypes.StringType, true)
			});
	

	//Create DataSet from stream messages from kafka
    Dataset<VideoEventData> ds = spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"))
      .option("subscribe", prop.getProperty("kafka.topic"))
      .option("kafka.max.partition.fetch.bytes", prop.getProperty("kafka.max.partition.fetch.bytes"))
      .option("kafka.max.poll.records", prop.getProperty("kafka.max.poll.records"))
      .load()
      .selectExpr("CAST(value AS STRING) as message")
      .select(functions.from_json(functions.col("message"),schema).as("json"))
      .select("json.*")
      .as(Encoders.bean(VideoEventData.class)); 
    
    //key-value pair of cameraId-VideoEventData
	KeyValueGroupedDataset<String, VideoEventData> kvDataset = ds.groupByKey(new MapFunction<VideoEventData, String>() {
		@Override
		public String call(VideoEventData value) throws Exception {
			// Init camera windows
			initCameraWindows(value);
			return value.getCameraId();
		}
	}, Encoders.STRING());

	//process
	Dataset<VideoEventData> processedDataset = kvDataset.mapGroupsWithState(new MapGroupsWithStateFunction<String, VideoEventData, VideoEventData,VideoEventData>(){
		@Override
		public VideoEventData call(String key, Iterator<VideoEventData> values, GroupState<VideoEventData> state) throws Exception {
			logger.warn("CameraId="+key+" PartitionId="+TaskContext.getPartitionId());
			VideoEventData existing = null;
			//check previous state
			if (state.exists()) {
				existing = state.get();
			}
			//detect motion
			VideoEventData processed = VideoMotionDetector.detectMotion(key,values,processedImageDir,existing);
			
			//update last processed
			if(processed != null){
				state.update(processed);
			}
			return processed;
		}}, Encoders.bean(VideoEventData.class), Encoders.bean(VideoEventData.class));

	//start
	 StreamingQuery query = processedDataset.writeStream()
		      .outputMode("update")
		      .format("console")
		      .start();
	 
	 //await
     query.awaitTermination();
	}
}


