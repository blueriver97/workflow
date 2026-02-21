#!/bin/bash

echo "INFO: Replace env in core-site.xml ..."
sed -i "s#\$S3_BUCKET#$S3_BUCKET#g" /opt/hadoop/etc/hadoop/core-site.xml
sed -i "s#\$AWS_ENDPOINT_URL_S3#$AWS_ENDPOINT_URL_S3#g" /opt/hadoop/etc/hadoop/core-site.xml
sed -i "s#\$AWS_ACCESS_KEY_ID#$AWS_ACCESS_KEY_ID#g" /opt/hadoop/etc/hadoop/core-site.xml
sed -i "s#\$AWS_SECRET_ACCESS_KEY#$AWS_SECRET_ACCESS_KEY#g" /opt/hadoop/etc/hadoop/core-site.xml
sed -i "s#\$YARN_RM_HOSTNAME_RM01#$YARN_RM_HOSTNAME_RM01#g" /opt/hadoop/etc/hadoop/yarn-site.xml
sed -i "s#\$YARN_RM_HOSTNAME_RM02#$YARN_RM_HOSTNAME_RM02#g" /opt/hadoop/etc/hadoop/yarn-site.xml
sed -i "s#\$YARN_RM_ADDRESS_RM01#$YARN_RM_ADDRESS_RM01#g" /opt/hadoop/etc/hadoop/yarn-site.xml
sed -i "s#\$YARN_RM_ADDRESS_RM02#$YARN_RM_ADDRESS_RM02#g" /opt/hadoop/etc/hadoop/yarn-site.xml
sed -i "s#\$YARN_RM_SCHEDULER_ADDRESS_RM01#$YARN_RM_SCHEDULER_ADDRESS_RM01#g" /opt/hadoop/etc/hadoop/yarn-site.xml
sed -i "s#\$YARN_RM_SCHEDULER_ADDRESS_RM02#$YARN_RM_SCHEDULER_ADDRESS_RM02#g" /opt/hadoop/etc/hadoop/yarn-site.xml

echo "INFO: Replace env in spark-defaults.conf ..."
sed -i "s#\$S3_BUCKET#$S3_BUCKET#g" /opt/spark/conf/spark-defaults.conf
