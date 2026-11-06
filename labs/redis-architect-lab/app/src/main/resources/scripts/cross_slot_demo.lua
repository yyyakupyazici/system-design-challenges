-- Cluster gösterimi: iki key'e dokunan script. Key'ler farklı slottaysa Redis Cluster CROSSSLOT hatası verir.
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], ARGV[1])
return 1
