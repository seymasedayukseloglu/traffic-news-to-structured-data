import requests

import xml.etree.ElementTree as ET

import csv

import time

import random

from datetime import datetime

from urllib.parse import quote



def get_total_turkiye_2023_data():

    # 1. 81 İL LİSTESİ

    iller = [

        "Adana", "Adıyaman", "Afyonkarahisar", "Ağrı", "Amasya", "Ankara", "Antalya", "Artvin", "Aydın", "Balıkesir",

        "Bilecik", "Bingöl", "Bitlis", "Bolu", "Burdur", "Bursa", "Çanakkale", "Çankırı", "Çorum", "Denizli",

        "Diyarbakır", "Edirne", "Elazığ", "Erzincan", "Erzurum", "Eskişehir", "Gaziantep", "Giresun", "Gümüşhane", "Hakkari",

        "Hatay", "Isparta", "Mersin", "İstanbul", "İzmir", "Kars", "Kastamonu", "Kayseri", "Kırklareli", "Kırşehir",

        "Kocaeli", "Konya", "Kütahya", "Malatya", "Manisa", "Kahramanmaraş", "Mardin", "Muğla", "Muş", "Nevşehir",

        "Niğde", "Ordu", "Rize", "Sakarya", "Samsun", "Siirt", "Sinop", "Sivas", "Tekirdağ", "Tokat",

        "Trabzon", "Tunceli", "Şanlıurfa", "Uşak", "Van", "Yozgat", "Zonguldak", "Aksaray", "Bayburt", "Karaman",

        "Kırıkkale", "Batman", "Şırnak", "Bartın", "Ardahan", "Iğdır", "Yalova", "Karabük", "Kilis", "Osmaniye", "Düzce"

    ]

   

    # 2. 12 AY (ZAMAN DİLİMLERİ)

    aylar = [

        ("2023-01-01", "2023-01-31"), ("2023-02-01", "2023-02-28"),

        ("2023-03-01", "2023-03-31"), ("2023-04-01", "2023-04-30"),

        ("2023-05-01", "2023-05-31"), ("2023-06-01", "2023-06-30"),

        ("2023-07-01", "2023-07-31"), ("2023-08-01", "2023-08-31"),

        ("2023-09-01", "2023-09-30"), ("2023-10-01", "2023-10-31"),

        ("2023-11-01", "2023-11-30"), ("2023-12-01", "2023-12-31")

    ]

   

    terimler = [

        "kaza", "carpisma", "çarpışma", "çarpt", "çarpışt", "devrildi",

        "devrilme", "takla", "şarampol", "bariyer", "yaralı", "ölü", "öldü", "yaralı", "yaralandı"

        "vefat", "can verdi", "yaşamını yitirdi", "şehit", "uçtu", "düştü",

        "yuvarlandı", "facia", "katliam", "zincirleme", "otobüs devrilmesi", "hayatını kaybetti", "devril"

    ]

   

    all_news = []

    seen_links = set()

    total_queries = len(iller) * len(aylar) * len(terimler)

    current_query = 0



    print(f"{'='*70}")

    print(f" TEZ PROJESİ: 2023 TÜRKİYE GENELİ MAKSİMUM VERİ TARAMASI")

    print(f" Toplam Hedef Sorgu Sayısı: {total_queries}")

    print(f" Tahmini Süre: 2-4 Saat")

    print(f"{'='*70}")



    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}



    try:

        for il in iller:

            print(f"\n>>> Şehir: {il.upper()} işleniyor...")

            for ay_no, (bas, bit) in enumerate(aylar, 1):

                for terim in terimler:

                    current_query += 1

                    query = f"{il} {terim} after:{bas} before:{bit}"

                    encoded_q = quote(query)

                    url = f"https://news.google.com/rss/search?q={encoded_q}&hl=tr&gl=TR&ceid=TR:tr"

                   

                    try:

                        resp = requests.get(url, headers=headers, timeout=15)

                        if resp.status_code == 200:

                            root = ET.fromstring(resp.content)

                            items = root.findall(".//item")

                           

                            new_found = 0

                            for item in items:

                                link = item.find("link").text

                                if link not in seen_links:

                                    seen_links.add(link)

                                    title = item.find("title").text

                                    all_news.append({

                                        "il": il,

                                        "ay": ay_no,

                                        "baslik": title.rsplit(" - ", 1)[0],

                                        "kaynak": title.split(" - ")[-1] if " - " in title else "Bilinmiyor",

                                        "link": link,

                                        "tarih": item.find("pubDate").text

                                    })

                                    new_found += 1

                           

                            if current_query % 50 == 0: # Her 50 sorguda bir durum özeti

                                print(f"    [İlerleme: {current_query}/{total_queries}] Toplam Veri: {len(all_news)}")

                       

                        # Google IP engeli yememek için rastgele kısa bekleme

                        time.sleep(random.uniform(0.3, 0.7))

                       

                    except Exception as e:

                        continue



    except KeyboardInterrupt:

        print("\n[!] İşlem durduruldu. Mevcut veriler kaydediliyor...")



    # 4. KAYDETME

    if all_news:

        ts = datetime.now().strftime("%Y%m%d_%H%M")

        filename = f"TEZ_2023_81_IL_MAX_VERI_{ts}.csv"

        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:

            w = csv.DictWriter(f, fieldnames=all_news[0].keys())

            w.writeheader()

            w.writerows(all_news)

       

        print("\n" + "="*70)

        print(f" İŞLEM TAMAMLANDI")

        print(f" Toplam Benzersiz Haber: {len(all_news)}")

        print(f" Dosya: {filename}")

        print(f"{'='*70}")

    else:

        print("\n[!] Hiç veri toplanamadı.")



if __name__ == "__main__":

    get_total_turkiye_2023_data() 