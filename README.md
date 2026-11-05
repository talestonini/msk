# MSK Sandpit

## Bastion

    BASTION=$(aws cloudformation describe-stacks --stack-name basic-msk \
      --query "Stacks[0].Outputs[?OutputKey=='BastionInstanceId'].OutputValue" --output text)
    aws ssm start-session --target "$BASTION"
    bash -l


## Schemas

    aws glue create-schema --registry-id RegistryName=basic-msk-cluster-registry \
      --schema-name temperature-changed --data-format AVRO --compatibility BACKWARD \
      --schema-definition file://avro/temperature-changed.avsc


## Topics

    kafka-topics.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) \
      --command-config ~/msk/admin.properties \
      --create --topic temperature-changed \
      --partitions 3 --replication-factor 2

    kafka-topics.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) \
      --command-config ~/msk/admin.properties --describe --topic temperature-changed

    -- producer
    kafka-acls.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties \
      --add --allow-principal User:example-app --producer --topic temperature-changed

    -- consumer (replace the group with the one your app uses)
    kafka-acls.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties \
      --add --allow-principal User:example-app --consumer --topic temperature-changed --group temperature-consumers
