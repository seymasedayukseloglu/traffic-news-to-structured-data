<h1 align="center">
🚦 Yapay Zekâ Destekli Büyük Veri Mimarisi ile Trafik Kazası Haberlerinden Yapılandırılmış Veri Üretimi ve Analizi
</h1>

<p align="center">
Lisans Tezi – Yönetim Bilişim Sistemleri <br>
Bursa Uludağ Üniversitesi, İnegöl İşletme Fakültesi
</p>

<hr>

<h2>📖 Proje Hakkında</h2>

<p>
Bu proje, Türkiye genelindeki trafik kazası haberlerinden otomatik olarak yapılandırılmış veri üretmek amacıyla geliştirilmiştir.
Google News RSS kaynaklarından toplanan yüz binlerce haber kaydı; büyük veri teknolojileri, doğal dil işleme teknikleri ve büyük dil modelleri kullanılarak işlenmektedir.
</p>

<p>
Sistem, trafik kazası haberlerini diğer haberlerden ayırmakta, haber içeriklerinden olay bilgilerini çıkarmakta ve analiz edilebilir JSON kayıtlarına dönüştürmektedir.
</p>

<p>
Çalışma kapsamında yaklaşık <b>488.000 haber</b> işlenmiş, bunlardan <b>147.533</b> trafik kazası haberi tespit edilmiş ve nihayetinde <b>55.448 doğrulanmış yapılandırılmış trafik kazası kaydı</b> elde edilmiştir.
</p>

<hr>

<h2>🎯 Amaç</h2>

<ul>
<li>Google News RSS üzerinden trafik kazası haberlerini toplamak</li>
<li>Google yönlendirme bağlantılarını gerçek haber adreslerine çözümlemek</li>
<li>Trafik kazası haberlerini otomatik sınıflandırmak</li>
<li>LLM kullanarak haberlerden yapılandırılmış bilgi çıkarmak</li>
<li>Büyük veri mimarisi ile gerçek zamanlı veri akışı sağlamak</li>
<li>Analize hazır trafik kazası veri kümesi oluşturmak</li>
</ul>

<hr>

<h2>🏗 Sistem Mimarisi</h2>

<pre>
Google News RSS
        │
        ▼
Apache Kafka
        │
        ▼
Spark Structured Streaming
        │
        ▼
Haber Filtreleme
(RegEx + LLM)
        │
        ▼
Bilgi Çıkarımı
(Qwen3 + LM Studio)
        │
        ▼
JSON Veri Kümesi
        │
        ▼
Analiz ve Görselleştirme
</pre>

<hr>

<h2>⚙️ Kullanılan Teknolojiler</h2>

<ul>
<li>Python</li>
<li>Apache Kafka</li>
<li>Apache Spark Structured Streaming</li>
<li>Google News RSS</li>
<li>Playwright</li>
<li>LM Studio</li>
<li>Qwen3 LLM</li>
<li>Pandas</li>
<li>JSON</li>
<li>CSV</li>
<li>Regex</li>
</ul>

<hr>

<h2>📂 Proje Bileşenleri</h2>

<h3>1️⃣ Veri Toplama</h3>

<p>
Google News RSS kaynaklarından Türkiye'nin 81 ili için haberlerin toplanması.
</p>

<pre>
google_rss_kafka_producer.py
</pre>

<h3>2️⃣ Link Çözümleme</h3>

<p>
Google News yönlendirme bağlantılarının gerçek haber adreslerine dönüştürülmesi.
</p>

<pre>
resolve_links.py
</pre>

<h3>3️⃣ Trafik Kazası Sınıflandırması</h3>

<p>
Regex kuralları ve LLM kullanılarak haberlerin trafik kazası olup olmadığının belirlenmesi.
</p>

<pre>
llm_filter.py
</pre>

<h3>4️⃣ Yapılandırılmış Bilgi Çıkarımı</h3>

<p>
Haber metinlerinden aşağıdaki alanların çıkarılması:
</p>

<ul>
<li>Özet</li>
<li>Tarih</li>
<li>İl / Lokasyon</li>
<li>Kaza Türü</li>
<li>Araç Türü</li>
<li>Ölü Sayısı</li>
<li>Yaralı Sayısı</li>
<li>Kaza Sebebi</li>
<li>Hava Durumu</li>
</ul>

<pre>
summarize.py
</pre>

<h3>5️⃣ Gerçek Zamanlı Büyük Veri İşleme</h3>

<p>
Kafka üzerinden gelen haberlerin Spark Structured Streaming ile işlenmesi.
</p>

<pre>
spark_trafik_pipeline.py
</pre>

<hr>

<h2>📊 Veri Akışı</h2>

<pre>
Google News RSS
      ↓
Kafka Producer
      ↓
Kafka Topic
      ↓
Spark Consumer
      ↓
Trafik Kazası Filtreleme
      ↓
LLM Bilgi Çıkarımı
      ↓
JSON Dataset
</pre>

<hr>

<h2>📈 Üretilen Veri Yapısı</h2>

<pre>
{
  "tarih": "2025-05-14",
  "il": "Bursa",
  "lokasyon": "İnegöl",
  "kaza_turu": "Çarpışma",
  "arac_turu": "Otomobil",
  "olu_sayisi": 1,
  "yarali_sayisi": 3,
  "kaza_sebebi": "Kontrol Kaybı",
  "hava_durumu": "Yağmurlu",
  "ozet": "..."
}
</pre>

<hr>

<h2>📊 Temel Sonuçlar</h2>

<ul>
<li>Toplam haber kaydı: <b>487.746</b></li>
<li>Tespit edilen trafik kazası haberi: <b>147.533</b></li>
<li>Nihai yapılandırılmış kayıt: <b>55.448</b></li>
<li>En yaygın kaza türü: <b>Çarpışma</b></li>
<li>En sık karışan araç türü: <b>Otomobil</b></li>
<li>Ağır ticari araçlarda ölüm oranı daha yüksektir</li>
</ul>

<hr>

<h2>🚀 Kurulum</h2>

<pre>
git clone https://github.com/kullaniciadi/proje.git

cd proje

pip install -r requirements.txt
</pre>

<hr>

<h2>▶️ Çalıştırma</h2>

<h3>Kafka Producer</h3>

<pre>
python google_rss_kafka_producer.py
</pre>

<h3>Spark Streaming</h3>

<pre>
spark-submit spark_trafik_pipeline.py
</pre>

<h3>LLM Servisi</h3>

<pre>
LM Studio başlatılır
Qwen3 modeli yüklenir
Local API aktif edilir
</pre>

<hr>

<h2>📚 Akademik Bilgiler</h2>

<p>
<b>Tez Başlığı:</b><br>
Yapay Zekâ Destekli Büyük Veri Mimarisi ile Trafik Kazası Haberlerinden Yapılandırılmış Veri Üretimi ve Analizi
</p>

<p>
<b>Yazar:</b> Şeyma Seda Yükseloğlu<br>
<b>Danışman:</b> Prof. Dr. Melih Engin
</p>

<p>
Bu çalışma, trafik güvenliği araştırmaları için güncel ve ölçeklenebilir bir veri üretim yaklaşımı sunmaktadır. :contentReference[oaicite:0]{index=0}
</p>

<hr>

<h2>📄 Lisans</h2>

<p>
Bu proje akademik amaçlarla geliştirilmiştir.
</p>

<hr>

<p align="center">
⭐ Eğer projeyi faydalı bulduysanız repo'ya yıldız vermeyi unutmayın.
</p>
