# redash-mysql-shards

RedashでシャーディングされたDBでまとめてクエリを実行するデータソースです  
{param1} - {param4}まで4つの値をシャードごとに設定に埋め込むことができます

## インストール方法
* mysql_shards.py を `redash/query_runner/` にコピーします
* 環境変数を設定します`REDASH_ADDITIONAL_QUERY_RUNNERS=redash.query_runner.mysql_shards`
  * dockerの場合はdocker-compose.ymlの`environment`に追加するか、.envファイルに追加してください
  * docker-compose.ymlに追加する場合は `server`, `worker` の2つの設定に追加してください
* データソースアイコンを追加します
  * dockerの場合は `cp client/dist/images/db-logos/mysql.png client/dist/images/db-logos/mysql_shards.png`
