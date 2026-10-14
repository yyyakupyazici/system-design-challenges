# System Design Challenges

Günlük system design antrenmanı: her gün bir case, önce tartışma, sonra önerilen tasarım ve çalışan kod.

## Akış

1. **Challenge** – Senaryo, somut gereksinimler (QPS, veri hacmi, latency, SLA, AI caselerinde token/s ve maliyet), bilerek konmuş zorluklar.
2. **Benim tasarımım** – Veri modeli, DB seçimi, bileşenler, ağ topolojisi, trade-off'lar.
3. **Tartışma** – Eksikler, edge case'ler, "ölçek x10 olursa?" soruları.
4. **Önerilen tasarım** – Mimari, şema, API'ler, akışlar, alternatifler.
5. **Kod** – Spring Boot 3 (Java 21) + Kubernetes manifestleri + testler.

## Kategoriler

- **Klasik:** veritabanı modelleme, ağ/altyapı, dağıtık sistemler, ölçeklenme, mesajlaşma, güvenilirlik, gözlemlenebilirlik, güvenlik.
- **AI:** RAG, vector DB, LLM gateway, LLM serving / GPU scheduling, agent orkestrasyonu, guardrails, eval & tracing, ML altyapısı. Kod Spring AI ile yazılır; testler mock modelle anahtarsız çalışır.

## Klasör yapısı

```
challenges/
  NNN-konu-adi/
    README.md   # senaryo, gereksinimler, benim yorumum, önerilen tasarım, trade-off'lar, nasıl çalıştırılır
    design/     # Mermaid diyagramları
    app/        # Spring Boot projesi (Maven)
    k8s/        # Kubernetes manifestleri
INDEX.md        # tüm challenge'ların listesi
```

Tüm challenge'lar için bkz. [INDEX.md](INDEX.md).
