# MSK Sandpit

## Bastion Setup

    BASTION=$(aws cloudformation describe-stacks --stack-name basic-msk \
      --query "Stacks[0].Outputs[?OutputKey=='BastionInstanceId'].OutputValue" --output text)
    aws ssm start-session --target "$BASTION"
    bash -l

    -- setup an admin user
    msk-setup

    -- add all permissions to the admin user
    kafka-acls.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) \
      --command-config ~/msk/admin.properties \
      --add --allow-principal User:admin --operation All \
      --cluster --topic '*' --group '*' --transactional-id '*'

    -- redeploy narrowing ACLs
    aws cloudformation deploy --template-file iac/msk-cluster.yaml --stack-name basic-msk --capabilities \
      CAPABILITY_IAM --parameter-overrides AllowEveryoneIfNoAclFound=false


## Schemas

    -- create schema
    aws glue create-schema --registry-id RegistryName=basic-msk-cluster-registry \
      --schema-name com.example.events.TemperatureChanged --data-format AVRO --compatibility BACKWARD \
      --schema-definition file://avro/temperature-changed.avsc


## Topics

    -- create topic
    kafka-topics.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties \
      --create --topic com.example.events.TemperatureChanged --partitions 3 --replication-factor 2

    -- describe topic
    kafka-topics.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties \
      --describe --topic com.example.events.Temperature

    -- add producer ACLs
    kafka-acls.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties \
      --add --allow-principal User:example-app --producer --topic com.example.events.TemperatureChanged \
      --transactional-id temperature-producer-1

    -- add consumer ACLs
    kafka-acls.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties \
      --add --allow-principal User:example-app --consumer --topic com.example.events.TemperatureChanged \
      --group temperature-consumers


## Run client

    -- consumer
    java -cp target/temperature-clients.jar com.example.TemperatureConsumer consumer.properties
    
    -- producer (in another terminal)
    java -cp target/temperature-clients.jar com.example.TemperatureProducer producer.properties 10