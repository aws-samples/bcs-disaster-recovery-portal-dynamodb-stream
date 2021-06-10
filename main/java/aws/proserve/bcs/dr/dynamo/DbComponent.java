// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.dynamo;


import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.annotation.Target;
import aws.proserve.bcs.dr.lambda.annotation.TaskToken;
import aws.proserve.bcs.dr.secret.Credential;
import aws.proserve.bcs.dr.secret.SecretManager;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import dagger.BindsInstance;
import dagger.Component;

import javax.annotation.Nullable;
import javax.inject.Singleton;

@Singleton
@Component(modules = DbModule.class)
interface DbComponent {

    static Credential getCredential(String secretId) {
        return DaggerDbComponent.builder()
                .build()
                .secretManager()
                .getCredential(secretId);
    }

    SecretManager secretManager();

    Worker replicateWorker();

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder sourceTable(@SourceTable String sourceTable);

        @BindsInstance
        Builder targetTable(@TargetTable String targetTable);

        @BindsInstance
        Builder taskToken(@Nullable @TaskToken String taskToken);

        @BindsInstance
        Builder sourceRegion(@Source String region);

        @BindsInstance
        Builder targetRegion(@Target String region);

        /**
         * @apiNote In-partition replication does not need credential.
         */
        @BindsInstance
        Builder sourceCredential(@Nullable @Source AWSCredentialsProvider provider);

        DbComponent build();
    }
}
