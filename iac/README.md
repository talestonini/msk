# MSK Cluster

- **VPC:** a new VPC (`10.0.0.0/16`) with DNS turned on, which MSK needs.
- **Subnets:** two private subnets (`10.0.1.0/24` and `10.0.2.0/24`), each in a different Availability Zone. The broker count stays at 2, one per subnet.
- **Login (authentication):**
  - SASL/SCRAM on port 9096, always over TLS. IAM and unauthenticated access are both off.
  - The template creates the Secrets Manager secret for the `admin` user with a generated password. MSK requires the secret name to start with `AmazonMSK_` and to be encrypted with your own KMS key, so the template creates that key too.
  - The secret is linked to the cluster with `AWS::MSK::BatchScramSecret`.
- **Permissions (authorization):** Kafka ACLs, controlled by `allow.everyone.if.no.acl.found` (see below).
- **Security group:** only allows port 9096 from inside the VPC.
- **Topic creation:** I turned off automatic topic creation, since topics should be created deliberately once ACLs are in use. Set `auto.create.topics.enable` back to `true` in the template if you'd rather keep it.

**Granting permissions after deploy:**
`AllowEveryoneIfNoAclFound` defaults to `true` on purpose. If it were `false` from the start, the admin user would have no permissions and couldn't create the first ACL. So:

1. Deploy:
   ```bash
   aws cloudformation deploy --template-file iac/msk-cluster.yaml --stack-name basic-msk
   ```
2. Get the password with `aws secretsmanager get-secret-value --secret-id <ScramSecretArn>`, and write a `client.properties` file:
   ```properties
   security.protocol=SASL_SSL
   sasl.mechanism=SCRAM-SHA-512
   sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="<password>";
   ```
3. Give admin full rights:
   ```bash
   kafka-acls.sh --bootstrap-server <brokers> --command-config client.properties --add \
     --allow-principal User:admin --operation All --cluster --topic '*' --group '*'
   ```
   Add ACLs for any other users at this point too.
4. Lock it down: redeploy with `--parameter-overrides AllowEveryoneIfNoAclFound=false`. CloudFormation creates a new configuration revision and applies it to the brokers with a rolling restart.

Clients have to run inside this VPC, because the template creates no internet gateway, NAT gateway, or way in from your network. To test from your machine you'd need an EC2 instance in one of the subnets, or VPC peering or a VPN. I can add a small bastion host if that's useful.


---


I've added the bastion host to [iac/msk-cluster.yaml](iac/msk-cluster.yaml). CloudFormation's `validate-template` accepts the template, but I haven't deployed it. I also couldn't print the boot script the instance will get, because the YAML library isn't installed here. That script is untested until the first deploy.

**What was added:**
- **Network:** a public subnet (`10.0.0.0/24`) with an internet gateway. Only the bastion uses it, to reach Session Manager and download Kafka. The two MSK subnets stay private.
- **Bastion:** a `t3.micro` running the latest Amazon Linux 2023, with an encrypted disk and the newer, token-based instance metadata service (IMDSv2) required.
- **Access:** you connect through SSM Session Manager. There's no SSH key, and its security group allows no inbound traffic.
- **Permissions:** its IAM role has Session Manager access plus only what it needs for MSK: reading the SCRAM secret, decrypting it with the KMS key, and looking up the broker addresses.
- **Software:** at boot it installs Java 17 and the Kafka 3.8.1 command-line tools (I checked the download link works), and puts them on the `PATH`.
- **Helper script:** `msk-setup` writes `~/client.properties` (only you can read it) and `~/bootstrap.txt` (the broker addresses).

**How to use it:**
```bash
aws cloudformation deploy --template-file iac/msk-cluster.yaml --stack-name basic-msk \
  --capabilities CAPABILITY_IAM

# Use the BastionConnectCommand stack output (needs the Session Manager plugin installed locally):
aws ssm start-session --target <BastionInstanceId>

# On the bastion:
msk-setup
kafka-acls.sh --bootstrap-server $(cat ~/bootstrap.txt) --command-config ~/client.properties --add \
  --allow-principal User:admin --operation All --cluster --topic '*' --group '*'
```
After that, redeploy with `AllowEveryoneIfNoAclFound=false` as before.

The bastion is created only after the MSK cluster finishes, because its permissions reference the cluster. That doesn't make the stack take any longer, since the cluster is the slowest part anyway.


---


What you're describing is **mutual TLS (mTLS)**. That's the other open-source Kafka login method MSK supports, but it isn't what the template does. I chose SASL/SCRAM, which uses a username and password. Both methods feed into Kafka ACLs the same way. They differ only in how a client proves who it is.

## Both methods end with a principal

Kafka ACLs are granted to a principal such as `User:something`. The login method decides where that name comes from:

| | SASL/SCRAM (current template) | mTLS (what you described) |
|---|---|---|
| Client proves identity with | username + password | client certificate + private key |
| Principal | `User:<username>` | `User:<certificate distinguished name>` |
| MSK port | 9096 | 9094 |
| TLS | One-way: the client checks the broker's certificate. That certificate comes from Amazon's public CA, so Java trusts it with no extra setup. | Two-way: the broker also checks the client's certificate. |
| Credentials stored in | Secrets Manager (`AmazonMSK_*` secrets) | Issued by an AWS Private CA |
| Extra cost | about $0.40/month per secret | Private CA: about $400/month (general-purpose mode), or less in short-lived mode, where you renew certificates every few days |
| Rotation | Change the secret. | Reissue and redeploy certificates before they expire. |

The ACL commands are identical except for the principal name. With SCRAM, the topic and group permissions you'd grant per app work exactly as you pictured. Only the credential is different.

## What the `admin` user is for

It's a **bootstrap and admin identity, not something client apps should share.** You use it from the bastion to:
1. Create the first ACLs while `allow.everyone.if.no.acl.found=true`.
2. Create topics and grant permissions to the other users.
3. Keep it for later admin work, such as new topics, new ACLs, and checking consumer groups.

## How client apps would work under SCRAM

Each app gets **its own** SCRAM user: one secret per app, all linked to the cluster. You then grant each one only what it needs, for example:
```bash
# orders-producer may only write to the orders topic
kafka-acls.sh ... --add --allow-principal User:orders-producer --producer --topic orders
# billing-consumer may only read orders, in its own consumer group
kafka-acls.sh ... --add --allow-principal User:billing-consumer --consumer --topic orders --group billing
```
The producer or consumer configuration is the same three lines as `client.properties`, with that app's username and password. The app should read them from its own secret at startup rather than from a file in the codebase:
```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="orders-producer" password="...";
```

## Your call

- **Keep SCRAM:** I'd change the template to take a list of app usernames and create one secret per user, all linked to the cluster. That's cheaper and simpler to run.
- **Switch to mTLS:** I'd add an AWS Private CA and turn on TLS client login. Apps would then use a keystore with their certificate instead of a password. MSK can run SCRAM and mTLS side by side, so you could keep SCRAM just for `admin`.

Which do you want?


---


I've set up the template for one SCRAM user per app instead of a list of usernames. CloudFormation only allows one secret association per MSK cluster, and it must list every secret's ARN. Its loop feature can create one secret per name, but it can't collect their ARNs into that one list. So each app user is now an explicit block in [iac/msk-cluster.yaml](iac/msk-cluster.yaml). `validate-template` accepts the template; I haven't deployed it.

**What changed:**
- **Admin user:** the parameter `ScramUsername` is now `AdminUsername`, and its secret is `AdminScramSecret`. It's described as an operator account for the bastion, not for client apps. The bastion's `msk-setup` script now reads this secret.
- **Example app user:** `ExampleAppScramSecret` creates a user called `example-app`, whose Kafka principal is `User:example-app`. It's a placeholder: rename it to a real app or delete it.
- **Secret association:** `MskScramSecretAssociation` lists both secrets. A comment notes that it must list every SCRAM secret.
- **Outputs:** the ARN of each secret, plus the KMS key ARN, which apps need in order to decrypt their own secret.

**Adding an app** (the steps are also written as a comment in the template):
1. Copy the `ExampleAppScramSecret` block, giving it a new logical ID and username.
2. Add a `!Ref` to the new secret in `MskScramSecretAssociation.SecretArnList`, then redeploy.
3. From the bastion, grant it only what it needs:
   ```bash
   kafka-acls.sh --bootstrap-server $(cat ~/bootstrap.txt) --command-config ~/client.properties \
     --add --allow-principal User:example-app --producer --topic orders
   ```
4. Give the app's IAM role (its ECS task, Lambda or EC2 role) `secretsmanager:GetSecretValue` on its own secret and `kms:Decrypt` on the key. The app reads its username and password at startup and puts them in its producer or consumer settings:
   ```properties
   security.protocol=SASL_SSL
   sasl.mechanism=SCRAM-SHA-512
   sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="example-app" password="<from secret>";
   ```

IAM is used here only so the app can read its own password from Secrets Manager. Kafka login and permissions are pure SCRAM and ACLs.


---


About **US$31 for a week (168 hours)** in Sydney, at on-demand prices I pulled from the AWS Pricing API just now.

| Item | Price (Sydney) | 1 week |
|---|---|---|
| MSK brokers: 2 × `kafka.t3.small` | $0.0578 per broker-hour | **$19.42** |
| MSK storage: 2 × 10 GB | $0.12 per GB-month | $0.55 |
| Bastion: `t3.medium` | $0.0528 per hour | **$8.87** |
| Bastion disk: 20 GB gp3 | $0.096 per GB-month | $0.44 |
| Bastion public IPv4 address | $0.005 per hour | $0.84 |
| KMS key | $1 per month | $0.23 |
| 2 SCRAM secrets (admin, example-app) | $0.40 per secret-month | $0.18 |
| **Total** | | **≈ $30.50** |

**Free or negligible:** the VPC, subnets, internet gateway and security groups; Session Manager; MSK's `DEFAULT` monitoring; CloudFormation; and replication traffic between brokers inside MSK. Downloads to the bastion are also free, since inbound data isn't charged.

**Costs that depend on how you use it:**
- **Cross-AZ client traffic:** about $0.01/GB each way when a client talks to a broker in the other AZ. That's pennies at test volumes.
- **Bastion CPU bursting:** `t3` instances default to "unlimited" mode, so long periods of high CPU (such as constant builds) add about $0.05 per vCPU-hour above the baseline.

**Ways to cut it:**
- **Stop the bastion when you're not using it:** that saves most of its ~$9/week, leaving only the disk at about $0.44/week. The public IP is released while it's stopped.
- **Use a smaller bastion:** `BastionInstanceType=t3.small` saves about $4.40/week, at the risk of slower or memory-starved Maven and .NET builds.
- **Delete the stack between sessions:** MSK clusters can't be stopped, so the brokers (about two-thirds of the bill) keep charging until the cluster is deleted. The secrets are deleted with it, so apps would get new passwords next time.

The background container test of the bastion setup (installing the packages, then building the sample Java and C# SCRAM clients) is still running. I'll report the results when it finishes.


---


Yes, the bastion now has what you need to build and run Java and C# Kafka apps. I tested its boot script in an Amazon Linux 2023 container (the same OS as the bastion), and both a Java and a C# SCRAM producer built and started. I haven't run them against a real cluster.

**What I changed in [iac/msk-cluster.yaml](iac/msk-cluster.yaml):**
- **Toolchains:** the bastion now installs the Java 21 JDK and Maven 3.8.4, the .NET 10 SDK, and `git`. It already had the Kafka 3.8.1 command-line tools.
- **Size:** the default instance is now `t3.medium` (4 GiB RAM), and the disk is 20 GB, so builds don't run out of memory or space. This is the size my cost estimate assumed.
- **Kafka download:** it now saves to a file first, with timeouts and retries. My first test run hung on a stalled download from `archive.apache.org`, and the old version would have hung the real bastion's boot the same way.
- **Permissions:** the bastion can now read any `AmazonMSK_<cluster>_*` secret, not just admin's, so you can run apps as their own users.
- **`msk-setup [username]`:** with no argument it uses the admin user. It writes these files, readable only by you:
  - `~/msk/bootstrap.txt`: the broker addresses.
  - `~/msk/<user>.properties`: settings for the Kafka tools and Java clients.
  - `~/msk/<user>.env`: `MSK_BOOTSTRAP_SERVERS`, `MSK_SCRAM_USERNAME` and `MSK_SCRAM_PASSWORD`, which any app can read.

  This replaces the earlier `~/client.properties` and `~/bootstrap.txt`, so the ACL command from before now needs `--bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties`.

**What the test showed:**
- **Java:** a Maven project using `org.apache.kafka:kafka-clients:3.8.1` compiled and printed `JAVA OK: SCRAM producer created`.
- **C#:** a `dotnet new console` project with the `Confluent.Kafka` package (librdkafka 2.16.0) printed `CSHARP OK: SCRAM producer created`. Its "connection refused" errors are expected, because there's no broker in the container.
- **Not tested:** a real login and TLS connection to MSK. That needs the deployed cluster.

**Things to know when you write the apps:**
- **Java:** Maven 3.8.4 uses an old compiler plugin by default that ignores the Java version setting. Pin `maven-compiler-plugin` to 3.13.0 in your `pom.xml`, or the build fails with "Source option 5 is no longer supported".
- **C#:** if you get certificate errors connecting to the brokers, set `SslCaLocation = "/etc/pki/tls/certs/ca-bundle.crt"`. I couldn't test this without a cluster.
- **Running an app:** use `msk-setup example-app && source ~/msk/example-app.env && <run your app>`.

`validate-template` still accepts the template, and deploying still needs `--capabilities CAPABILITY_IAM`. The test files are in `~/.msk-bastion-test` and `/tmp/msk-bastion-test`, outside the repo, and can be deleted.