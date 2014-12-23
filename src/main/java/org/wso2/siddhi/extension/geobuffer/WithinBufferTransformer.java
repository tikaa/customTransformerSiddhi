/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.extension.geobuffer;

import org.geotools.geometry.jts.JTSFactoryFinder;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@SiddhiExtension(namespace = "geo", function = "withinbuffertransformer")
public class WithinBufferTransformer extends TransformProcessor {

	/**
	 * 
	 */
	private static final double DEGREES_TO_METERS_CONVERSION_CONSTANT = 110574.61087757687;
	private static final int paramThree = 2; // indexes for accessing param list
	private static final int paramTwo = 1;
	private static final int paramOne = 0;
	private static final String COORDINATES = "coordinates";
	private static final String GEOMETRY = "geometry";
	private static final String FEATURES = "features";
	private static final String WITHIN_TIME = "withinTime";
	private static final String WITHIN_POINT = "withinPoint";
	private static final String WITHIN_FLAG = "withinFlag";
	private static final String SPEED_FLAG = "speedFlag";
	private static final String SPEED = "speed";
	private static final String LATITUDE = "lat";
	private static final String LONGITUDE = "longitude";
	private static final String TIME = "time";
	private static final String ID = "id";
	private static final String OUTPUT_STREAM = "outputStream";

	private Geometry[] bufferList;
	private JsonArray jmLocCoordinatesArray;
	private HashMap<String, String[]> deviceLocMap = new HashMap<String, String[]>();
	private double giventime = 10.0; // if not defined 10 seconds as default
	private double givenRadius = 1.0; // if not defined take 1 meter as default
	private int pointLength = 0;

	public void destroy() {
	}

	@Override
	protected InStream processEvent(InEvent event) {
		// id,time,longitude,lat,speed, false as speedFlag
		String withinPoint = "null";
		/*
		 * this assignment was done purposely since if not assigned needs to
		 * remain as string null
		 */
		boolean withinState = false;
		boolean withinTime = false;
		int id = Integer.parseInt(event.getData0().toString());
		String strId = event.getData0().toString();
		double time = Double.parseDouble(event.getData1().toString());
		String strTime = event.getData1().toString();
		double lat = Double.parseDouble(event.getData3().toString());
		double longitude = Double.parseDouble(event.getData2().toString());
		double speed = Double.parseDouble(event.getData4().toString());
		double timeDiff = 0;
		int buffers = 0;
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		Coordinate checkCoord = new Coordinate(lat, longitude);
		Point checkPoint = geometryFactory.createPoint(checkCoord);
		for (buffers = 0; buffers < pointLength; buffers++) {
			// for each of the buffers drawn
			Geometry currBuffer = bufferList[buffers];
			if (checkPoint.within(currBuffer)) {
				// check whether the point is within
				withinState = true;
				JsonObject jmObject = (JsonObject) jmLocCoordinatesArray.get(buffers);
				JsonObject tempJmObject = jmObject.getAsJsonObject(GEOMETRY);
				JsonArray coordArray = tempJmObject.getAsJsonArray(COORDINATES);
				withinPoint = coordArray.get(0).toString() + "," + coordArray.get(1).toString();
				/*
				 * put it into Hashmap as the latest within true event for the
				 * id if hashmap doesnt contain that id already
				 */
				if (deviceLocMap.containsKey(strId) == false
						|| deviceLocMap.containsKey(strId) == true
						&& deviceLocMap.get(strId)[1].equalsIgnoreCase(currBuffer.toString()) == false) {
					String[] currArray = { strTime, currBuffer.toString() };
					deviceLocMap.put(strId, currArray);
				} else {
					timeDiff = time - Double.parseDouble(deviceLocMap.get(strId)[0]);
					if (timeDiff >= giventime) {
						withinTime = true;
					}
				}
			} else {
				if (deviceLocMap.containsKey(strId) == true) {
					deviceLocMap.remove(strId);
				}
			}
		}
		Object[] data = new Object[] { id, time, lat, longitude, speed, false, withinState,
				withinPoint, withinTime };
		return new InEvent(event.getStreamId(), System.currentTimeMillis(), data);
	}

	@Override
	protected InStream processEvent(InListEvent listEvent) {
		return null;
	}

	@Override
	protected Object[] currentState() {
		return null;
	}

	@Override
	protected void restoreState(Object[] data) {
	}

	@Override
	protected void init(Expression[] parameters, List<ExpressionExecutor> expressionExecutors,
			StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition,
			String elementId, SiddhiContext siddhiContext) {
		// initiating at the beginning to avoid overhead
		ArrayList<Object> paramList = new ArrayList<Object>();
		// access data from stream for processing
		for (int i = 0, size = expressionExecutors.size(); i < size; i++) {
			paramList.add(expressionExecutors.get(i).execute(null));
		}
		// creating the buffer points
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		String coordString = paramList.get(paramOne).toString();
		if (paramList.get(1) != null) {
			giventime = (Double) paramList.get(paramTwo);
			/*
			 *  to convert into latitude degrees from metric measurements since the calculations are done in
			 *  geo-space 
			 */
			givenRadius = (Double) paramList.get(paramThree) / DEGREES_TO_METERS_CONVERSION_CONSTANT; 
		}
		JsonElement mcoordinateArray = new JsonParser().parse(coordString);
		JsonObject jmObject = mcoordinateArray.getAsJsonObject();
		jmLocCoordinatesArray = jmObject.getAsJsonArray(FEATURES);
		pointLength = jmLocCoordinatesArray.size();
		bufferList = new Geometry[jmLocCoordinatesArray.size()];
		int elemNo = 0;
		for (JsonElement jmLocElem : jmLocCoordinatesArray) {
			// getting the geometry feature			
			JsonObject jObject = (JsonObject) jmLocElem;
			JsonObject geometryObject = jObject.getAsJsonObject(GEOMETRY);
			JsonArray coordArray = geometryObject.getAsJsonArray(COORDINATES);
			double lattitude = Double.parseDouble(coordArray.get(0).toString());
			double longitude = Double.parseDouble(coordArray.get(1).toString());
			// inserting for passing to UI
			Coordinate coord = new Coordinate(lattitude, longitude);
			Point point = geometryFactory.createPoint(coord);
			Geometry buffer = point.buffer(givenRadius); // draw the buffer
			bufferList[elemNo] = buffer; // put it into the list
			elemNo++;
		}
		// defining the output Stream
		this.outStreamDefinition = new StreamDefinition().name(OUTPUT_STREAM)
				.attribute(ID, Attribute.Type.INT).attribute(TIME, Attribute.Type.DOUBLE)
				.attribute(LONGITUDE, Attribute.Type.DOUBLE)
				.attribute(LATITUDE, Attribute.Type.DOUBLE).attribute(SPEED, Attribute.Type.DOUBLE)
				.attribute(SPEED_FLAG, Attribute.Type.BOOL)
				.attribute(WITHIN_FLAG, Attribute.Type.BOOL)
				.attribute(WITHIN_POINT, Attribute.Type.STRING)
				.attribute(WITHIN_TIME, Attribute.Type.BOOL);

	}

}
