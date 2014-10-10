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

package customtransformer;

import java.util.ArrayList;
import java.util.List;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

//this is a test class to test the extension
public class WithinBufferTest {

	public static void main(String[] args) throws Exception {

		SiddhiConfiguration conf = new SiddhiConfiguration();
		List<Class> classList = new ArrayList<Class>();
		classList.add(WithinBufferTransformer.class);
		conf.setSiddhiExtensions(classList);

		SiddhiManager siddhiManager = new SiddhiManager();
		siddhiManager.getSiddhiContext().setSiddhiExtensions(classList);
		siddhiManager.defineStream("define stream cseEventStream (id int, time double, lattitude double, longitude double, speed double ) ");
		/*
		 * format of the data being sent {'geometries':[{ 'type': 'Point',
		 * 'coordinates': [100.5, 0.5] },{ 'type': 'Point', 'coordinates':
		 * [100.5, 0.5] }]} {"geometries":[{"type":"Point","coordinates":[
		 * 79.94248329162588
		 * ,6.844997820293952]},{"type":"Point","coordinates":[100.0,0.0]}]}
		 */
		String queryReference = siddhiManager
				.addQuery("from "
						+ "cseEventStream#transform.geo:withinbuffertransformer(\""
						+ "{'features':[{ 'type': 'Feature', 'properties':{},'geometry':{'type': 'Point', 'coordinates': "
						+ "[  79.94248329162588,6.844997820293952] }}]}\") as tt "
						+ "insert into StockQuote;");
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
			}
		});
		// id,time,longitude,lat,speed, false as speedFlag
		InputHandler inputHandler = siddhiManager.getInputHandler("cseEventStream");
		inputHandler.send(new Object[] { 1, 234.345, 100.786, 6.9876, 98.34 });
		Thread.sleep(100);
		siddhiManager.shutdown();
	}

}
