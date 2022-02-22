/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.mjp.vertx;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VertxServer {

    public void startServer(int port, String host){
        Vertx vertx = Vertx.vertx();
        Router router = Router.router(vertx);
        router.get("/metrics").handler(request -> {
            HttpServerResponse response = request.response();
            response.send("success !!!!");
        });

        Vertx.vertx().deployVerticle(new AbstractVerticle() {
            @Override
            public void start() throws Exception {
               vertx.createHttpServer().requestHandler(router).listen(port, host);
            }
        });

    }

}
