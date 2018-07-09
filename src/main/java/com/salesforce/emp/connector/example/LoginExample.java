/* 
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license. 
 * For full license text, see LICENSE.TXT file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.emp.connector.example;

import static com.salesforce.emp.connector.LoginHelper.login;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.eclipse.jetty.util.ajax.JSON;

import com.salesforce.emp.connector.BayeuxParameters;
import com.salesforce.emp.connector.EmpConnector;
import com.salesforce.emp.connector.LoginHelper;
import com.salesforce.emp.connector.TopicSubscription;

/**
 * An example of using the EMP connector using login credentials
 *
 * @author hal.hildebrand
 * @since 202
 */
public class LoginExample {
    public static void main(String[] argv) throws Exception {
        if (argv.length < 3 || argv.length > 4) {
            System.err.println("Usage: LoginExample username password topic [replayFrom]");
            System.exit(1);
        }
        long replayFrom = EmpConnector.REPLAY_FROM_EARLIEST;
        if (argv.length == 4) {
            replayFrom = Long.parseLong(argv[3]);
        }

        BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
            try {
                return login(argv[0], argv[1]);
            } catch (Exception e) {
                e.printStackTrace(System.err);
                System.exit(1);
                throw new RuntimeException(e);
            }
        });

        BayeuxParameters params = tokenProvider.login();

        // Program args: username password topic replayId
        // replayId:
        //  -1:  Tip of the queue (no past events)
        //  -2:  Plays past events.  Events are stored in SF for 24 hours
        //  <actualId>: play from this id forward (non-inclusive)
        // example: sfptdev.vicuna.lex@patrontechnology.com <password or password+token> /event/DebugEvent__e -1

        //Consumer<Map<String, Object>> consumer = event -> System.out.println(String.format("Received:\n%s", JSON.toString(event)));
        Consumer<Map<String, Object>> consumer = event -> {
            HashMap<String, Object> hash = (HashMap)event;
            HashMap<String, Object> evt = (HashMap)hash.get("event");
            Long replayId = (Long)evt.get("replayId");

            HashMap<String, Object> payload = (HashMap)hash.get("payload");

            System.out.println(String.format("==================== \n== Received: %s \n====================", replayId));
            System.out.println(String.format("\t Message: %s ", payload.get("Message__c")));
            System.out.println(String.format("\t Timestamp: %s ", payload.get("Timestamp__c")));
            System.out.println(String.format("\t Type: %s ", payload.get("Type__c")));
            System.out.println(String.format("\t Detail: %s ", payload.get("Detail__c")));
            System.out.println();
        };

        EmpConnector connector = new EmpConnector(params);

        connector.setBearerTokenProvider(tokenProvider);

        connector.start().get(5, TimeUnit.SECONDS);

        TopicSubscription subscription = connector.subscribe(argv[2], replayFrom, consumer).get(5, TimeUnit.SECONDS);

        System.out.println(String.format("Subscribed: %s", subscription));
    }
}
