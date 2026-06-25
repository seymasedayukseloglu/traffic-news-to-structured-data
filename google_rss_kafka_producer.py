from kafka import KafkaProducer 

import json, uuid, requests 

import xml.etree.ElementTree as ET 

from urllib.parse import quote 

  

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

terimler = [ 

    "trafik kazası", "kaza", "çarpışma", "zincirleme kaza", 

    "devrildi", "takla attı", "yaralı", "ölümlü kaza", 

    "hayatını kaybetti", "otobüs devrildi", 

    "kamyon devrildi", "motosiklet kazası" 

] 

  

producer = KafkaProducer( 

    bootstrap_servers="localhost:9092", 

    value_serializer=lambda v: 

        json.dumps(v, ensure_ascii=False).encode("utf-8") 

) 

  

seen_links = set()                        

for il in iller: 

    for terim in terimler: 

        query = f"{il} {terim} after:{start_date} before:{end_date}" 

        rss_url = ("https://news.google.com/rss/search?q=" 

                   + quote(query) + "&hl=tr&gl=TR&ceid=TR:tr") 

        response = requests.get(rss_url, timeout=20) 

        root = ET.fromstring(response.content) 

        for item in root.findall(".//item"): 

            link = item.find("link").text 

            if link in seen_links: 

                continue 

            seen_links.add(link) 

            news = { 

                "id": str(uuid.uuid4()), 

                "il": il, 

                "terim": terim, 

                "baslik": item.find("title").text, 

                "link": link, 

                "tarih": item.find("pubDate").text 

            } 

            producer.send("trafik_haberleri_raw", value=news) 

  

producer.flush()                       

 