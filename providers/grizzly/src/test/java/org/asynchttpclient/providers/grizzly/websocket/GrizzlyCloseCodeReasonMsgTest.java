/*
 * Copyright (c) 2012 Sonatype, Inc. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package org.asynchttpclient.providers.grizzly.websocket;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.providers.grizzly.GrizzlyProviderUtil;
import org.asynchttpclient.websocket.CloseCodeReasonMessageTest;
import org.testng.annotations.Test;

public class GrizzlyCloseCodeReasonMsgTest extends CloseCodeReasonMessageTest {

    @Override
    public AsyncHttpClient getAsyncHttpClient(AsyncHttpClientConfig config) {
        return GrizzlyProviderUtil.grizzlyProvider(config);
    }

    @Override
    @Test
    public void onCloseWithCode() throws Throwable {
        super.onCloseWithCode();    //To change body of overridden methods use File | Settings | File Templates.
    }
}
