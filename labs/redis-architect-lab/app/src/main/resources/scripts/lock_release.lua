-- Kilidi SADECE sahibi bırakabilir. "GET sonra DEL" iki çağrı olsaydı: GET'ten sonra kilidin süresi dolup
-- başkası alabilir, sonra bizim DEL başkasının kilidini silerdi. Karşılaştır-ve-sil tek adımda olmalı.
-- KEYS[1] kilit   ARGV[1] sahip token'ı   Dönüş: 1 bırakıldı | 0 kilit artık bizim değil
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
