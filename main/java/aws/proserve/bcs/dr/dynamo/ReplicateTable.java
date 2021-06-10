// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;

import aws.proserve.bcs.dr.lambda.VoidHandler;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import aws.proserve.bcs.dr.secret.Credential;
import com.amazonaws.services.lambda.runtime.Context;

import java.util.Objects;

public class ReplicateTable implements VoidHandler<ReplicateTable.Request> {

    /**
     * @apiNote entry point of fargate.
     */
    public static void main(String[] args) {
        new ReplicateTable().handleRequest(getRequest(), null);
        System.exit(0); // explicit exit for fargate
    }

    private static Request getRequest() {
        final var request = new Request();
        final var source = new Resource();
        final var target = new Resource();

        source.setName(env("source_table"));
        source.setRegion(env("source_region"));
        target.setName(env("target_table"));
        target.setRegion(env("target_region"));
        request.setToken(env("task_token"));

        request.setSource(source);
        request.setTarget(target);
        request.setSecretId(System.getenv("secret_id"));
        return request;
    }

    private static String env(String key) {
        return Objects.requireNonNull(System.getenv(key), key + " cannot be null.");
    }

    @Override
    public void handleRequest(Request request, Context context) {
        final var credential = DbComponent.getCredential(request.getSecretId());
        final var component = DaggerDbComponent.builder()
                .sourceTable(request.getSource().getName())
                .sourceRegion(request.getSource().getRegion())
                .sourceCredential(Credential.toProvider(credential))
                .targetTable(request.getTarget().getName())
                .targetRegion(request.getTarget().getRegion())
                .taskToken(request.getToken())
                .build();
        component.replicateWorker().run();
    }

    static class Request {
        private Resource source;
        private Resource target;
        private String secretId;
        private String token;

        public Resource getSource() {
            return source;
        }

        public void setSource(Resource source) {
            this.source = source;
        }

        public Resource getTarget() {
            return target;
        }

        public void setTarget(Resource target) {
            this.target = target;
        }

        public String getSecretId() {
            return secretId;
        }

        public void setSecretId(String secretId) {
            this.secretId = secretId;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }
    }
}
