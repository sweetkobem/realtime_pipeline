#!/bin/bash
echo "Generating override for default user config..."

cat > /etc/clickhouse-server/users.d/override-default-user.xml <<EOF
<clickhouse>
  <users>
    <${CLICKHOUSE_USER}>
      <password>${CLICKHOUSE_PASSWORD}</password>
      <networks>
        <ip>::/0</ip>
      </networks>
    </${CLICKHOUSE_USER}>
  </users>
</clickhouse>
EOF
