import json
import argparse
import os
import requests
import datetime
import re
import time
import threading
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import quote_plus, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime as dt


JSON_FILE = "lovestory.json"
LOG_FILE = "scraper_log.json"
FALLBACK_ICON = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/myicon.png"


SOURCES = {
    "ninoiptv": "https://ninoiptv.com/",
    "iptvcodes": "https://www.iptvcodes.online/",
    "m3umax": "https://m3umax.blogspot.com/",
    "worldiptv": "https://world-iptv.club/"
}


MIN_PLAYLIST_SIZE = 500
MIN_STREAM_COUNT = 10
PLAYLIST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_DELAY = 2
DEFAULT_VALIDATION_WORKERS = 10
DEFAULT_DISCOVERY_WORKERS = 10
MAX_WORKERS_LIMIT = 24
ZERO_LINK_REINSPECT_HOURS = 12

THREAD_LOCAL = threading.local()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}


GOT_SLOTS = [
    {"name": "Westeros", "logo": "https://static.digitecgalaxus.ch/im/Files/2/1/1/3/9/7/9/6/game_of_thrones_intro_map_westeros_elastic21.jpeg?impolicy=teaser&resizeWidth=1000&resizeHeight=500"},
    {"name": "Essos", "logo": "https://imgix.bustle.com/uploads/image/2017/7/12/4e391a2f-8663-4cdd-91eb-9102c5f731d7-52be1751932bb099d5d5650593df5807b50fc3fbbee7da6a556bd5d1d339f39a.jpg?w=800&h=532&fit=crop&crop=faces"},
    {"name": "Beyond the Wall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/27/706_Tormund_Beric_Sandor_Jon_Jorah_Gendry.jpg/revision/latest/scale-to-width-down/1000?cb=20170821092659"},
    {"name": "Winterfell", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/08/1x01_Winterfell.jpg/revision/latest?cb=20170813191451"},
    {"name": "King's Landing", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/83/King%27s_Landing_HotD.png/revision/latest/scale-to-width-down/1000?cb=20220805155800"},
    {"name": "The Wall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f5/The_Wall.jpg/revision/latest/scale-to-width-down/1000?cb=20150323200738"},
    {"name": "Castle Black", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7b/Castle_Black.jpg/revision/latest/scale-to-width-down/1000?cb=20110920111941"},
    {"name": "Dragonstone", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a4/Dragonstone-season7-low.png/revision/latest/scale-to-width-down/1000?cb=20170717082952"},
    {"name": "Highgarden", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a2/704_Highgarden.png/revision/latest?cb=20170807030944"},
    {"name": "Casterly Rock", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a8/Casterly-rock.png/revision/latest/scale-to-width-down/1000?cb=20170731025431"},
    {"name": "The Eyrie", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/59/The_Eyrie.jpg/revision/latest?cb=20110615190250"},
    {"name": "Dorne", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f5/Dorne.png/revision/latest/scale-to-width-down/1000?cb=20120719190909"},
    {"name": "Braavos", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7b/Titan_of_Braavos.jpg/revision/latest/scale-to-width-down/1000?cb=20150504024857"},
    {"name": "Meereen", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/89/Meereen.png/revision/latest/scale-to-width-down/1000?cb=20150328211743"},
    {"name": "Valyria", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/19/Valyria_5x05_%284%29.png/revision/latest/scale-to-width-down/1000?cb=20150511133123"},
    {"name": "White Harbor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1a/WhiteHarbor.jpg/revision/latest/scale-to-width-down/1000?cb=20140218212230"},
    {"name": "Red Keep", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/71/S8_Red_Keep.jpg/revision/latest?cb=20190305171013"},
    {"name": "Riverrun", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/66/Riverrun._battlements.png/revision/latest/scale-to-width-down/1000?cb=20160606102912"},
    {"name": "Harrenhal", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9b/Harrenhal.jpg/revision/latest/scale-to-width-down/1000?cb=20150328214605"},
    {"name": "Storm's End", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/25/Storm%27s_End_Official_Guide.jpg/revision/latest?cb=20221024024717"},
    {"name": "Oldtown", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/01/BtD_AIH_Oldtown.png/revision/latest/scale-to-width-down/1000?cb=20220805155230"},
    {"name": "The Citadel", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/4e/S06E10_-_The_Citadel.png/revision/latest?cb=20160627153650"},
    {"name": "The Iron Islands", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/eb/Iron_Islands.png/revision/latest?cb=20120719194710"},
    {"name": "Pyke", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9d/Pyke.jpg/revision/latest?cb=20120402161004"},
    {"name": "The Reach", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/89/The_Reach.png/revision/latest/scale-to-width-down/1000?cb=20120719200419"},
    {"name": "The North", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2b/Distant_Winterfell.jpg/revision/latest/scale-to-width-down/1000?cb=20220201081605"},
    {"name": "The Iron Throne", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c8/Iron_throne.jpg/revision/latest/scale-to-width-down/1000?cb=20131005175755"},
    {"name": "The Red Woman", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/0d/The_night_is_dark_and_full_of_terrors.jpg/revision/latest/scale-to-width-down/1000?cb=20160802075640"},
    {"name": "The Great Sept of Baelor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d9/GreatSeptS3.jpg/revision/latest/scale-to-width-down/1000?cb=20150412170141"},
    {"name": "Night's Watch", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/25/Jon%2C_Sam_and_Pyp.jpg/revision/latest/scale-to-width-down/1000?cb=20101128160538"},
    {"name": "Kingsguard", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9e/Targ_Kingsguard.png/revision/latest?cb=20160419222717"},
    {"name": "The Free Cities", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/38/Longbridge.png/revision/latest/scale-to-width-down/1000?cb=20150328214200"},
    {"name": "The Narrow Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/6c/S04E10_-_Arya_%28on_the_ship%29.png/revision/latest/scale-to-width-down/1000?cb=20140618063538"},
    {"name": "Caraxes", "logo": "https://staticg.sportskeeda.com/editor/2022/12/03519-16722910130144-1920.jpg"},
    {"name": "Bear Island", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/16/Bear-island-02.jpg/revision/latest/scale-to-width-down/1000?cb=20160606033558"},
    {"name": "Sunspear", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b6/Sunspear-0.png/revision/latest?cb=20170918150538"},
    {"name": "Astapor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/ca/Astapor_city_view.jpg/revision/latest/scale-to-width-down/1000?cb=20150328211103"},
    {"name": "Balerion The Black Dread", "logo": "https://staticg.sportskeeda.com/editor/2022/12/62cdf-16722910130058-1920.jpg"},
    {"name": "Qarth", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/60/Qarth2.jpg/revision/latest/scale-to-width-down/1000?cb=20140422045311"},
    {"name": "Daenerys", "logo": "https://www.thepopverse.com/_next/image?url=https%3A%2F%2Fmedia.thepopverse.com%2Fmedia%2Femilia-clarke-game-of-thrones-dothraki-language-i2yd4ljq4zpfvikmv0onmgmvy2.png&w=1280&q=75"},
    {"name": "Slaver's Bay", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c5/Slaver%27s_Bay.png/revision/latest/scale-to-width-down/1000?cb=20120719201919"},
    {"name": "The Dothraki Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/67/S6_Horse_Statues.png/revision/latest/scale-to-width-down/1000?cb=20170819032739"},
    {"name": "The Trident", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/6d/Riverlands_map.jpg/revision/latest/scale-to-width-down/1000?cb=20110314205331"},
    {"name": "Godswood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ea/Godtree_S8_EP2.jpg/revision/latest/scale-to-width-down/1000?cb=20190417202906"},
    {"name": "Heart Tree", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/de/Old_Gods_infobox.jpg/revision/latest?cb=20130130103158"},
    {"name": "Weirwood Trees", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/cf/Winterfell_Godswood.jpg/revision/latest/scale-to-width-down/1000?cb=20110302144610"},
    {"name": "The Old Gods", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3d/Black_and_White_Weirwood_face.jpg/revision/latest?cb=20160716022634"},
    {"name": "The Seven", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7e/501_Great_Sept_Seven_statues.jpg/revision/latest/scale-to-width-down/1000?cb=20150416164510"},
    {"name": "The Lord of Light", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/0e/Black_and_White_Lord_of_Light.jpg/revision/latest/scale-to-width-down/1000?cb=20150608200849"},
    {"name": "White Walkers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5a/WhiteWalkersandArmy.png/revision/latest/scale-to-width-down/1000?cb=20211009053915"},
    {"name": "The Long Night", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/75/Grey_Worm_S8_Ep3.jpg/revision/latest/scale-to-width-down/1000?cb=20190429030211"},
    {"name": "Dragonglass", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/85/Dragonglass_Spear.jpg/revision/latest/scale-to-width-down/1000?cb=20150407175621"},
    {"name": "Valyrian Steel", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/08/Viserys_Dagger_3.png/revision/latest?cb=20221011000312"},
    {"name": "Wildlings", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/32/Ygritte.jpg/revision/latest?cb=20120524071810"},
    {"name": "Syrax", "logo": "https://staticg.sportskeeda.com/editor/2022/12/934cb-16722910129818-1920.jpg"},
    {"name": "Children of the Forest", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3b/Childrenoftheforest.jpg/revision/latest/scale-to-width-down/1000?cb=20160517184932"},
    {"name": "Faceless Men", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b0/Jaqen.png/revision/latest?cb=20160826000828"},
    {"name": "The Unsullied", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/51/UNSULLIEDS8.png/revision/latest/scale-to-width-down/1000?cb=20190517173434"},
    {"name": "The Dothraki", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e2/Dothraki_archery.jpg/revision/latest?cb=20180106203557"},
    {"name": "The Mountain", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/81/GregorGregorLightenedProfile.png/revision/latest?cb=20190724144505"},
    {"name": "Drogon", "logo": "https://staticg.sportskeeda.com/editor/2022/12/9eb54-16722910132128-1920.jpg"},
    {"name": "Rhaegal", "logo": "https://staticg.sportskeeda.com/editor/2022/12/ef298-16722910129506-1920.jpg"},
    {"name": "Viserion", "logo": "https://staticg.sportskeeda.com/editor/2022/12/2d488-16722910129839-1920.jpg"},
    {"name": "Ghost", "logo": "https://i.cbc.ca/ais/1.3078255,1431974097000/full/max/0/default.jpg?im=Crop%2Crect%3D%280%2C91%2C400%2C225%29%3BResize%3D860"},
    {"name": "Nymeria", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d3/Nymeria_bites_Joffrey.png/revision/latest/scale-to-width-down/1000?cb=20150404115158"},
    {"name": "The Three-Eyed Raven", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3a/Three-eyed_raven.png/revision/latest?cb=20110622101243"},
    {"name": "The Greyjoys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f7/House-Greyjoy-Main-Shield.png/revision/latest?cb=20170321185051"},
    {"name": "The Boltons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c8/House-Bolton-Main-Shield.png/revision/latest?cb=20170321185256"},
    {"name": "The Freys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/0d/House-Frey-Main-Shield.png/revision/latest?cb=20170321185357"},
    {"name": "The Tullys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/81/House-Tully-Main-Shield.png/revision/latest?cb=20170321185449"},
    {"name": "The Arryns", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2a/House-Arryn-Main-Shield.png/revision/latest?cb=20170321185539"},
    {"name": "The Martells", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f0/House-Martell-Main-Shield.png/revision/latest?cb=20170321185631"},
    {"name": "The Tyrells", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e4/House-Tyrell-Main-Shield.png/revision/latest?cb=20170321185722"},
    {"name": "The Baratheons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a0/House-Baratheon-Main-Shield.png/revision/latest?cb=20170321185807"},
    {"name": "The Mormonts", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/31/House-Mormont-Main-Shield.png/revision/latest?cb=20170321185851"},
    {"name": "The Umbers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9b/House-Umber-Main-Shield.png/revision/latest?cb=20170321185943"},
    {"name": "The Karstarks", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/80/House-Karstark-Main-Shield.png/revision/latest?cb=20170321190034"},
    {"name": "The Reeds", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ef/House-Reed-Main-Shield.png/revision/latest?cb=20170321190122"},
    {"name": "The Manderlys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/67/House-Manderly-Main-Shield.png/revision/latest?cb=20170321190212"},
    {"name": "The Glovers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5a/House-Glover-Main-Shield.png/revision/latest?cb=20170321190302"},
    {"name": "The Tarlys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5d/House-Tarly-Main-Shield.png/revision/latest?cb=20170321190347"},
    {"name": "The Blackwoods", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/21/House-Blackwood-Main-Shield.png/revision/latest?cb=20170321190435"},
    {"name": "The Royces", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a6/House-Royce-Main-Shield.png/revision/latest?cb=20170321190521"},
    {"name": "The Daynes", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/87/House-Dayne-Main-Shield.png/revision/latest?cb=20170321190606"},
    {"name": "The Hightowers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/92/House-Hightower-Main-Shield.png/revision/latest?cb=20170321190649"},
    {"name": "The Redwynes", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/ac/House-Redwyne-Main-Shield.png/revision/latest?cb=20170321190733"},
    {"name": "The Florents", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c5/House-Florent-Main-Shield.png/revision/latest?cb=20170321190817"},
    {"name": "The Conningtons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/91/House-Connington-Main-Shield.png/revision/latest?cb=20170321190903"},
    {"name": "The Yronwoods", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/55/House-Yronwood-Main-Shield.png/revision/latest?cb=20170321190948"},
    {"name": "The Brotherhood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/47/Brotherhood-without-banners-coat.png/revision/latest?cb=20170321191036"},
    {"name": "The Second Sons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/81/Second_Sons_Banner.jpg/revision/latest?cb=20170321191123"},
    {"name": "The Golden Company", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c9/Golden_Company_S8.png/revision/latest?cb=20190414034906"},
    {"name": "The Faith Militant", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d9/Faith_Militant.png/revision/latest?cb=20170321191211"},
    {"name": "The Brave Companions", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d3/Brave_Companions.png/revision/latest?cb=20170321191257"},
    {"name": "The Burned Men", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/8e/Burned_Men.png/revision/latest?cb=20170321191343"},
    {"name": "The Stone Crows", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5f/Stone_Crows.png/revision/latest?cb=20170321191428"},
    {"name": "The Moon Brothers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/63/Moon_Brothers.png/revision/latest?cb=20170321191513"},
    {"name": "The Black Ears", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a7/Black_Ears.png/revision/latest?cb=20170321191558"},
    {"name": "The Painted Dogs", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1f/Painted_Dogs.png/revision/latest?cb=20170321191643"},
    {"name": "The Sons of the Harpy", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/68/Sons_of_the_Harpy.png/revision/latest?cb=20170321191728"},
    {"name": "The Wise Masters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c1/Wise_Masters.png/revision/latest?cb=20170321191813"},
    {"name": "The Good Masters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f8/Good_Masters.png/revision/latest?cb=20170321191858"},
    {"name": "The Great Masters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e8/Great_Masters.png/revision/latest?cb=20170321191943"},
    {"name": "The Thirteen", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3c/The_Thirteen.png/revision/latest?cb=20170321192028"},
    {"name": "The Warlocks", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/dd/Warlocks.png/revision/latest?cb=20170321192113"},
    {"name": "The Alchemists", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5e/Alchemists.png/revision/latest?cb=20170321192158"},
    {"name": "The Maesters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a3/Maesters.png/revision/latest?cb=20170321192243"},
    {"name": "The Septons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/69/Septons.png/revision/latest?cb=20170321192328"},
    {"name": "The Silent Sisters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/cf/Silent_Sisters.png/revision/latest?cb=20170321192413"},
    {"name": "The Maesters' Chain", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/81/Maesters_Chain.png/revision/latest?cb=20170321192458"},
    {"name": "The Kingsroad", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7c/Kingsroad.png/revision/latest?cb=20170321192543"},
    {"name": "The Neck", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/96/The_Neck.png/revision/latest?cb=20170321192628"},
    {"name": "The Twins", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e9/The_Twins.jpg/revision/latest?cb=20170321192713"},
    {"name": "Moat Cailin", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d0/Moat_Cailin.png/revision/latest?cb=20170321192758"},
    {"name": "The Dreadfort", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2c/Dreadfort.png/revision/latest?cb=20170321192843"},
    {"name": "Last Hearth", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a9/Last_Hearth.png/revision/latest?cb=20170321192928"},
    {"name": "Karhold", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/64/Karhold.png/revision/latest?cb=20170321193013"},
    {"name": "The Citadel Library", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1f/Citadel_Library.png/revision/latest?cb=20170321193058"},
    {"name": "The House of Black and White", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c4/House_of_Black_and_White.jpg/revision/latest?cb=20170321193143"},
    {"name": "The Great Pyramid", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/8c/Great_Pyramid.png/revision/latest?cb=20170321193228"},
    {"name": "The Fighting Pits", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ed/Fighting_Pits.png/revision/latest?cb=20170321193313"},
    {"name": "The Temple of the Dosh Khaleen", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f7/Temple_of_Dosh_Khaleen.png/revision/latest?cb=20170321193358"},
    {"name": "Vaes Dothrak", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a2/Vaes_Dothrak.png/revision/latest?cb=20170321193443"},
    {"name": "The Shadow Lands", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/df/Shadow_Lands.png/revision/latest?cb=20170321193528"},
    {"name": "Asshai", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c9/Asshai.png/revision/latest?cb=20170321193613"},
    {"name": "The Fist of the First Men", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5d/Fist_of_First_Men.png/revision/latest?cb=20170321193658"},
    {"name": "Craster's Keep", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a0/Crasters_Keep.png/revision/latest?cb=20170321193743"},
    {"name": "The Cave of the Three-Eyed Raven", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/29/Cave_Three_Eyed_Raven.png/revision/latest?cb=20170321193828"},
    {"name": "The Land of Always Winter", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/78/Land_Always_Winter.png/revision/latest?cb=20170321193913"},
    {"name": "The God's Eye", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1e/Gods_Eye.png/revision/latest?cb=20170321193958"},
    {"name": "The Isle of Faces", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3f/Isle_of_Faces.png/revision/latest?cb=20170321194043"},
    {"name": "Greywater Watch", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d5/Greywater_Watch.png/revision/latest?cb=20170321194128"},
    {"name": "The Saltpans", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/87/Saltpans.png/revision/latest?cb=20170321194213"},
    {"name": "Seagard", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2d/Seagard.png/revision/latest?cb=20170321194258"},
    {"name": "Maidenpool", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f4/Maidenpool.png/revision/latest?cb=20170321194343"},
    {"name": "Darry", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a8/Darry.png/revision/latest?cb=20170321194428"},
    {"name": "Raventree Hall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/36/Raventree_Hall.png/revision/latest?cb=20170321194513"},
    {"name": "Stone Hedge", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e5/Stone_Hedge.png/revision/latest?cb=20170321194558"},
    {"name": "Acorn Hall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/72/Acorn_Hall.png/revision/latest?cb=20170321194643"},
    {"name": "The Inn at the Crossroads", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1c/Inn_Crossroads.png/revision/latest?cb=20170321194728"},
    {"name": "The Peach", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/dc/The_Peach.png/revision/latest?cb=20170321194813"},
    {"name": "Griffin's Roost", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/aa/Griffins_Roost.png/revision/latest?cb=20170321194858"},
    {"name": "Rain House", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/6f/Rain_House.png/revision/latest?cb=20170321194943"},
    {"name": "Bronzegate", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/4e/Bronzegate.png/revision/latest?cb=20170321195028"},
    {"name": "Greenstone", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c7/Greenstone.png/revision/latest?cb=20170321195113"},
    {"name": "Tarth", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9f/Tarth.png/revision/latest?cb=20170321195158"},
    {"name": "The Sapphire Isle", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2c/Sapphire_Isle.png/revision/latest?cb=20170321195243"},
    {"name": "The Arbor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5d/The_Arbor.png/revision/latest?cb=20170321195328"},
    {"name": "Horn Hill", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/31/Horn_Hill.png/revision/latest?cb=20170321195413"},
    {"name": "Brightwater Keep", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a1/Brightwater_Keep.png/revision/latest?cb=20170321195458"},
    {"name": "Bitterbridge", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7f/Bitterbridge.png/revision/latest?cb=20170321195543"},
    {"name": "Longtable", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/eb/Longtable.png/revision/latest?cb=20170321195628"},
    {"name": "Cider Hall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/29/Cider_Hall.png/revision/latest?cb=20170321195713"},
    {"name": "Ashford", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d0/Ashford.png/revision/latest?cb=20170321195758"},
    {"name": "The Water Gardens", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/4c/Water_Gardens.png/revision/latest?cb=20170321195843"},
    {"name": "Yronwood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/89/Yronwood.png/revision/latest?cb=20170321195928"},
    {"name": "Starfall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/0e/Starfall.png/revision/latest?cb=20170321200013"},
    {"name": "High Hermitage", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f7/High_Hermitage.png/revision/latest?cb=20170321200058"},
    {"name": "Skyreach", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/ce/Skyreach.png/revision/latest?cb=20170321200143"},
    {"name": "Vaith", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a5/Vaith.png/revision/latest?cb=20170321200228"},
    {"name": "Godsgrace", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1f/Godsgrace.png/revision/latest?cb=20170321200313"},
    {"name": "Lemonwood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3c/Lemonwood.png/revision/latest?cb=20170321200358"},
    {"name": "Spottswood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e7/Spottswood.png/revision/latest?cb=20170321200443"},
    {"name": "Salt Shore", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d9/Salt_Shore.png/revision/latest?cb=20170321200528"},
    {"name": "The Tor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f0/The_Tor.png/revision/latest?cb=20170321200613"},
    {"name": "Ghost Hill", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2c/Ghost_Hill.png/revision/latest?cb=20170321200658"},
    {"name": "Hellholt", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a0/Hellholt.png/revision/latest?cb=20170321200743"},
    {"name": "The Boneway", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/67/Boneway.png/revision/latest?cb=20170321200828"},
    {"name": "The Prince's Pass", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3f/Princes_Pass.png/revision/latest?cb=20170321200913"},
    {"name": "The Red Mountains", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ec/Red_Mountains.png/revision/latest?cb=20170321200958"},
    {"name": "The Tower of Joy", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7c/Tower_of_Joy.png/revision/latest?cb=20170321201043"},
    {"name": "The Stepstones", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d4/Stepstones.png/revision/latest?cb=20170321201128"},
    {"name": "The Summer Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f5/Summer_Sea.png/revision/latest?cb=20170321201213"},
    {"name": "The Smoking Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/86/Smoking_Sea.png/revision/latest?cb=20170321201258"},
    {"name": "The Shivering Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5a/Shivering_Sea.png/revision/latest?cb=20170321201343"},
    {"name": "The Sunset Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/39/Sunset_Sea.png/revision/latest?cb=20170321201428"},
    {"name": "The Jade Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ea/Jade_Sea.png/revision/latest?cb=20170321201513"},
    {"name": "Yi Ti", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c0/Yi_Ti.png/revision/latest?cb=20170321201558"},
    {"name": "Leng", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f8/Leng.png/revision/latest?cb=20170321201643"},
    {"name": "Qarth (City)", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d7/Qarth_City.png/revision/latest?cb=20170321201728"},
    {"name": "New Ghis", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a7/New_Ghis.png/revision/latest?cb=20170321201813"},
    {"name": "Volantis", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2e/Volantis.png/revision/latest?cb=20170321201858"},
    {"name": "Pentos", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f4/Pentos.png/revision/latest?cb=20170321201943"},
    {"name": "Myr", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5e/Myr.png/revision/latest?cb=20170321202028"},
    {"name": "Lys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a0/Lys.png/revision/latest?cb=20170321202113"},
    {"name": "Tyrosh", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b2/Tyrosh.png/revision/latest?cb=20170321202158"},
    {"name": "Norvos", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3d/Norvos.png/revision/latest?cb=20170321202243"},
    {"name": "Qohor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c5/Qohor.png/revision/latest?cb=20170321202328"},
    {"name": "Lorath", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/79/Lorath.png/revision/latest?cb=20170321202413"},
    {"name": "Yunkai", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a9/Yunkai.png/revision/latest?cb=20170321202458"}
]




def create_session():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_DELAY,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_domain(url):
    """Extract domain from URL"""
    try:
        parsed = urlparse(url)
        domain = (parsed.netloc or parsed.path.split('/')[0]).strip()
        # Normalize away embedded credentials so domain grouping is stable.
        domain = domain.split("@")[-1]
        return domain.replace('www.', '').lower()
    except:
        return "unknown"


def clean_credential_token(value):
    """Clean scraped credential values."""
    token = unescape(str(value or "")).strip().strip("\"'`")
    token = re.sub(r"[,\];|]+$", "", token).strip()
    return token


def normalize_xtream_base_url(raw_url):
    """Normalize a raw Xtream host value into a base URL."""
    base = clean_credential_token(raw_url)
    if not base:
        return ""
    if not re.match(r"^https?://", base, re.IGNORECASE):
        base = f"http://{base}"
    try:
        parsed = urlparse(base)
        if not parsed.netloc:
            return ""
        path = (parsed.path or "").rstrip("/")
        return f"{parsed.scheme}://{parsed.netloc}{path}"
    except Exception:
        return ""


def build_xtream_playlist_urls(base_url, username, password):
    """Build candidate playlist URLs from Xtream credentials."""
    username = clean_credential_token(username)
    password = clean_credential_token(password)
    if not base_url or not username or not password:
        return []
    user_q = quote_plus(username)
    pass_q = quote_plus(password)
    return [
        f"{base_url}/get.php?username={user_q}&password={pass_q}&type=m3u_plus&output=ts",
        f"{base_url}/get.php?username={user_q}&password={pass_q}&type=m3u_plus&output=m3u8",
    ]


def extract_xtream_credential_links(html_content):
    """Extract bare Xtream URL/User/Pass blocks and convert to playlist URLs."""
    text = BeautifulSoup(html_content or "", "html.parser").get_text("\n")
    text = unescape(text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    extracted = []
    current = {}
    last_seen_idx = -999

    url_pattern = re.compile(
        r"(?:url|host|server|portal)\b[^A-Za-z0-9]+"
        r"(https?://[^\s]+|[A-Za-z0-9.-]+:\d{2,5}[^\s]*)",
        re.IGNORECASE,
    )
    user_pattern = re.compile(
        r"(?:user(?:name)?|login)\b[^A-Za-z0-9]+([^\s]+)",
        re.IGNORECASE,
    )
    pass_pattern = re.compile(
        r"(?:pass(?:word)?|pwd)\b[^A-Za-z0-9]+([^\s]+)",
        re.IGNORECASE,
    )

    for idx, line in enumerate(lines):
        if current and idx - last_seen_idx > 6:
            current = {}

        url_match = url_pattern.search(line)
        if url_match:
            current["base_url"] = normalize_xtream_base_url(url_match.group(1))
            last_seen_idx = idx

        user_match = user_pattern.search(line)
        if user_match:
            current["username"] = clean_credential_token(user_match.group(1))
            last_seen_idx = idx

        pass_match = pass_pattern.search(line)
        if pass_match:
            current["password"] = clean_credential_token(pass_match.group(1))
            last_seen_idx = idx

        if all(key in current and current.get(key) for key in ("base_url", "username", "password")):
            extracted.extend(
                build_xtream_playlist_urls(
                    current["base_url"],
                    current["username"],
                    current["password"],
                )
            )
            # Keep base_url for repeated user/pass blocks on the same host.
            current = {"base_url": current["base_url"]}
            last_seen_idx = idx

    return extracted


def extract_m3u_links(html_content):
    """Extract M3U playlist URLs from HTML content"""
    html_content = unescape(html_content or "")
    links = []
    
    
    pattern1 = r'https?://[^\s<>"]+?(?:get\.php|player_api\.php)[^\s<>"]*'
    
    
    pattern2 = r'https?://[^\s<>"]+?\.m3u[^\s<>"]*'
    
    
    pattern3 = r'https?://[^\s<>"]+?(?:username|user)=[^\s<>"&]+(?:&|&amp;|&#038;)(?:password|pass)=[^\s<>"&]+'
    
    for pattern in [pattern1, pattern2, pattern3]:
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        links.extend(matches)

    # Convert bare Xtream credentials (URL/User/Pass blocks) into get.php playlist URLs.
    links.extend(extract_xtream_credential_links(html_content))
    
    # Clean up HTML entities and duplicates
    cleaned_links = []
    for link in links:
        # Decode HTML entities
        link = unescape(link.replace('&amp;', '&'))
        # Remove trailing punctuation
        link = re.sub(r'[.,;:)\]]+$', '', link)
        if link not in cleaned_links:
            cleaned_links.append(link)
    
    return cleaned_links


def extract_date_from_title(title):
    """Extract date from article title or URL (e.g., '26-01-2026')"""
  
    patterns = [
        r'(\d{2})[-/.](\d{2})[-/.](\d{4})',  
        r'(\d{4})[-/.](\d{2})[-/.](\d{2})',  
    ]
    
    for pattern in patterns:
        match = re.search(pattern, title)
        if match:
            try:
                g1, g2, g3 = match.groups()
                
                if len(g1) == 4:
                   
                    return dt.strptime(f"{g1}-{g2}-{g3}", "%Y-%m-%d")
                else:
                    
                    return dt.strptime(f"{g3}-{g2}-{g1}", "%Y-%m-%d")
            except:
                continue

    text_date_match = re.search(r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', title)
    if text_date_match:
        day, month_name, year = text_date_match.groups()
        text_value = f"{int(day):02d} {month_name} {year}"
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return dt.strptime(text_value, fmt)
            except ValueError:
                continue

    return None


def extract_version_from_title(title):
    """Extract version (V1, V2, etc.) from article title"""
    match = re.search(r'V(\d+)', title, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 1  


def validate_playlist(url, session):
    """Validate if playlist is live and count channels"""
    try:
        response = session.get(url, timeout=PLAYLIST_TIMEOUT, headers=HEADERS, allow_redirects=True)
        
        
        content_length = len(response.content)
        if content_length < MIN_PLAYLIST_SIZE:
            return False, f"Too small ({content_length} bytes)", 0
        
       
        content = response.text
        stream_count = content.count('#EXTINF')
        
        if stream_count < MIN_STREAM_COUNT:
            return False, f"Too few streams ({stream_count})", stream_count
        
        return True, "Valid", stream_count
        
    except requests.Timeout:
        return False, "Timeout", 0
    except Exception as e:
        return False, f"Error: {str(e)[:50]}", 0


def load_scraper_log():
    """Load scraping history"""
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"sources": {}}


def save_scraper_log(log_data):
    """Save scraping history"""
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False)


def get_name_for_slot(slot_id, domain):
    """Get name for slot from GOT_SLOTS or generate Untitled name"""
    if slot_id < len(GOT_SLOTS):
        return GOT_SLOTS[slot_id]["name"]
    # For slots beyond GOT_SLOTS, use Untitled
    untitled_number = slot_id - len(GOT_SLOTS) + 1
    return f"Untitled {untitled_number}"


def get_logo_for_slot(slot_id):
    """Get logo for slot from GOT_SLOTS or use fallback"""
    if slot_id < len(GOT_SLOTS):
        return GOT_SLOTS[slot_id]["logo"]
    return FALLBACK_ICON


def find_available_slot(used_slots):
    """
    Find an available slot ID, prioritizing:
    1. Empty slots within GOT_SLOTS range (for named slots)
    2. Next available slot after all existing slots
    """
   
    for i in range(len(GOT_SLOTS)):
        if i not in used_slots:
            return i, "named"
    
    
    max_id = max(used_slots) if used_slots else -1
    return max_id + 1, "untitled"


def rename_untitled_playlists(slot_registry):
    """
    Find Untitled playlists and move them to available named slots if possible
    Returns the number of playlists renamed
    """
    renamed_count = 0
    
   
    untitled_playlists = []
    for slot_id, item in slot_registry.items():
        if item.get("name", "").startswith("Untitled "):
            untitled_playlists.append((slot_id, item))
    
    if not untitled_playlists:
        return 0
    
    # Find available named slots
    available_named_slots = []
    for i in range(len(GOT_SLOTS)):
        if i not in slot_registry:
            available_named_slots.append(i)
    
    if not available_named_slots:
        return 0
    
    print(f"\n[RENAMING] Found {len(untitled_playlists)} Untitled playlists and {len(available_named_slots)} available named slots")
    
   
    for i, (old_slot_id, item) in enumerate(untitled_playlists):
        if i >= len(available_named_slots):
            break
        
        new_slot_id = available_named_slots[i]
        
       
        old_name = item["name"]
        new_name = GOT_SLOTS[new_slot_id]["name"]
        new_logo = GOT_SLOTS[new_slot_id]["logo"]
        
        item["slot_id"] = new_slot_id
        item["name"] = new_name
        item["logo_url"] = new_logo
        item["id"] = f"slot_{str(new_slot_id).zfill(3)}"
        item["last_changed"] = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        item["change_log"] = f"Renamed from '{old_name}' to '{new_name}' on {item['last_changed']}"
        
        
        del slot_registry[old_slot_id]
        slot_registry[new_slot_id] = item
        
        renamed_count += 1
        print(f"  [OK] Renamed: '{old_name}' -> '{new_name}' (Slot {old_slot_id} -> {new_slot_id})")
    
    return renamed_count




def scrape_ninoiptv(session, log_data):
    """Scrape ninoiptv.com with date and version tracking"""
    print("\n[SCRAPING] ninoiptv.com...")
    
    source_name = "ninoiptv"
    source_log = log_data["sources"].get(source_name, {"scraped_articles": {}})
    
    try:
        response = session.get(SOURCES["ninoiptv"], headers=HEADERS, timeout=15)
        print(f"  Response status: {response.status_code}")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        
        all_articles = soup.find_all('article')
        print(f"  DEBUG: Found {len(all_articles)} <article> tags")
        
        
        articles = []
        for idx, article in enumerate(all_articles):
           
            title_elem = article.find('h2', class_='entry-title')
            if not title_elem:
                title_elem = article.find('h1', class_='entry-title')
            if not title_elem:
               
                title_elem = article.find('h2')
            if not title_elem:
                title_elem = article.find('h1')
            
            if title_elem:
                link_elem = title_elem.find('a')
                if link_elem:
                    title = link_elem.get_text(strip=True)
                    url = link_elem.get('href')
                    
                    print(f"  DEBUG Article {idx+1}: {title[:80]}")
                    
                   
                    article_date = extract_date_from_title(title)
                    if not article_date:
                        article_date = extract_date_from_title(url)
                    
                    version = extract_version_from_title(title)
                    
                    if article_date:
                        articles.append({
                            'title': title,
                            'url': url,
                            'date': article_date,
                            'version': version
                        })
                        print(f"    -> Date: {article_date.strftime('%Y-%m-%d')}, Version: V{version}")
                    else:
                        print(f"    -> No date found in title or URL")
                else:
                    print(f"  DEBUG Article {idx+1}: No link found in title")
            else:
                print(f"  DEBUG Article {idx+1}: No title element found")
        
       
        articles.sort(key=lambda x: (x['date'], x['version']), reverse=True)
        
        print(f"\n  Found {len(articles)} dated articles")
        
        
        if not articles:
            print("  ERROR: No articles with dates found!")
            return []
        
        latest_date = articles[0]['date']
        latest_date_str = latest_date.strftime("%Y-%m-%d")
        
        
        latest_articles = [a for a in articles if a['date'] == latest_date]
        
        print(f"  Latest date: {latest_date_str}")
        versions_str = ', '.join([f'V{a["version"]}' for a in latest_articles])
        print(f"  Versions available: {versions_str}")
        
        all_links = []
        
        for article in latest_articles:
            article_key = f"{latest_date_str}_V{article['version']}"
            
            
            if article_key in source_log["scraped_articles"]:
                print(f"  [OK] Already scraped: {article['title']}")
                continue
            
            print(f"  -> Scraping: {article['title']}")
            
            try:
                article_response = session.get(article['url'], headers=HEADERS, timeout=15)
                article_soup = BeautifulSoup(article_response.content, 'html.parser')
                
                
                content = article_soup.find('div', class_='entry-content')
                if content:
                    html_text = str(content)
                    links = extract_m3u_links(html_text)
                    
                    print(f"    Found {len(links)} links")
                    all_links.extend(links)
                    
                    
                    source_log["scraped_articles"][article_key] = {
                        "title": article['title'],
                        "url": article['url'],
                        "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "links_found": len(links)
                    }
                else:
                    print(f"    ERROR: No entry-content div found")
                    
            except Exception as e:
                print(f"    ERROR scraping article: {type(e).__name__}: {str(e)}")
        
        
        log_data["sources"][source_name] = source_log
        
        print(f"  Total links extracted: {len(all_links)}")
        return all_links
        
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


def scrape_iptvcodes(session, log_data):
    """Scrape iptvcodes.online using Blogger JSON feed."""
    return scrape_blogger_feed(
        session=session,
        log_data=log_data,
        source_name="iptvcodes",
        display_name="iptvcodes.online",
        feed_url="https://www.iptvcodes.online/feeds/posts/default?alt=json&max-results=12",
        article_fallback_limit=1,
    )


def scrape_m3umax(session, log_data):
    """Scrape m3umax.blogspot.com using Blogger JSON feed."""
    return scrape_blogger_feed(
        session=session,
        log_data=log_data,
        source_name="m3umax",
        display_name="m3umax.blogspot.com",
        feed_url="https://m3umax.blogspot.com/feeds/posts/default?alt=json&max-results=12",
        article_fallback_limit=2,
    )


def normalize_source_log(log_data, source_name):
    """Ensure source log structure exists and is a dict."""
    if "sources" not in log_data or not isinstance(log_data["sources"], dict):
        log_data["sources"] = {}

    source_log = log_data["sources"].get(source_name)
    if not isinstance(source_log, dict):
        source_log = {}
    scraped_articles = source_log.get("scraped_articles")
    if not isinstance(scraped_articles, dict):
        scraped_articles = {}
    source_log["scraped_articles"] = scraped_articles
    return source_log


def safe_title_for_log(title, limit=80):
    """Make titles log-safe across terminals with limited encodings."""
    clean = re.sub(r"\s+", " ", str(title or "")).strip()
    if limit and len(clean) > limit:
        clean = clean[:limit]
    return clean.encode("ascii", "replace").decode("ascii")


def get_blogger_entry_url(entry):
    """Extract Blogger alternate URL from feed entry."""
    for link_item in entry.get("link", []):
        if link_item.get("rel") == "alternate":
            return link_item.get("href", "")
    return ""


def parse_log_timestamp(value):
    """Parse timestamp values stored in scraper_log.json."""
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return dt.strptime(value, fmt)
        except ValueError:
            continue
    return None


def should_reinspect_entry(existing_entry):
    """Re-check previously inspected entries if they yielded zero links and are stale."""
    if not isinstance(existing_entry, dict):
        return True

    try:
        links_found = int(existing_entry.get("links_found", 0) or 0)
    except Exception:
        links_found = 0

    if links_found > 0:
        return False

    last_time = parse_log_timestamp(existing_entry.get("scraped_at"))
    if not last_time:
        return True

    age_hours = (dt.now() - last_time).total_seconds() / 3600.0
    return age_hours >= ZERO_LINK_REINSPECT_HOURS


def write_source_log_entry(source_log, entry_key, title, url, links_count, extraction, existing_entry=None):
    """Persist per-entry inspection metadata for source feeds."""
    previous_count = 0
    if isinstance(existing_entry, dict):
        try:
            previous_count = int(existing_entry.get("inspection_count", 0) or 0)
        except Exception:
            previous_count = 0

    source_log["scraped_articles"][entry_key] = {
        "title": title,
        "url": url,
        "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
        "links_found": links_count,
        "extraction": extraction,
        "inspection_count": previous_count + 1,
        "last_result": "ok" if links_count > 0 else "no_links",
    }


def scrape_blogger_feed(session, log_data, source_name, display_name, feed_url, article_fallback_limit=0):
    """Feed-first strategy for Blogger sources with optional article fallback."""
    print(f"\n[SCRAPING] {display_name}...")
    source_log = normalize_source_log(log_data, source_name)

    try:
        response = session.get(feed_url, headers=HEADERS, timeout=20)
        print(f"  Feed status: {response.status_code}")
        feed = response.json().get("feed", {})
        entries = feed.get("entry", [])
    except Exception as e:
        print(f"  Error loading feed: {e}")
        return []

    print(f"  Feed entries fetched: {len(entries)}")
    target_day = entries[0].get("published", {}).get("$t", "")[:10] if entries else ""
    if target_day:
        entries = [
            entry for entry in entries
            if entry.get("published", {}).get("$t", "").startswith(target_day)
        ]
        print(f"  Feed entries selected: {len(entries)} for {target_day}")

    all_links = []
    fallback_fetches = 0
    new_entries = 0

    for entry in entries:
        title = entry.get("title", {}).get("$t", "Untitled")
        article_url = get_blogger_entry_url(entry)
        published = entry.get("published", {}).get("$t", "")
        article_key = article_url or f"{title}|{published}"
        existing_entry = source_log["scraped_articles"].get(article_key)

        if existing_entry and not should_reinspect_entry(existing_entry):
            continue

        content_html = (entry.get("content") or {}).get("$t", "")
        summary_html = (entry.get("summary") or {}).get("$t", "")
        links = extract_m3u_links(f"{content_html}\n{summary_html}")
        extraction = "feed"

        if not links and article_url and fallback_fetches < article_fallback_limit:
            try:
                fallback_fetches += 1
                article_response = session.get(article_url, headers=HEADERS, timeout=20)
                links = extract_m3u_links(article_response.text)
                extraction = "article"
            except Exception as e:
                print(f"  Fallback fetch failed for {safe_title_for_log(title, 50)}: {e}")

        all_links.extend(links)
        write_source_log_entry(
            source_log=source_log,
            entry_key=article_key,
            title=title,
            url=article_url,
            links_count=len(links),
            extraction=extraction,
            existing_entry=existing_entry,
        )
        new_entries += 1
        print(f"  -> {safe_title_for_log(title, 55)} | links={len(links)} via {extraction}")

    log_data["sources"][source_name] = source_log

    unique_links = len(dict.fromkeys(all_links))
    print(f"  New entries processed: {new_entries}")
    print(f"  Total links extracted: {len(all_links)} (unique={unique_links})")
    return all_links


def scrape_worldiptv(session, log_data):
    """Scrape world-iptv.club using RSS feed."""
    print("\n[SCRAPING] world-iptv.club...")
    source_name = "worldiptv"
    source_log = normalize_source_log(log_data, source_name)

    feed_url = SOURCES["worldiptv"].rstrip("/") + "/feed/"
    try:
        response = session.get(feed_url, headers=HEADERS, timeout=20)
        print(f"  Feed status: {response.status_code}")
        soup = BeautifulSoup(response.content, 'xml')
        items = soup.find_all('item')[:15]
    except Exception as e:
        print(f"  Error loading feed: {e}")
        return []

    print(f"  Feed items fetched: {len(items)}")
    target_date = None
    if items:
        first_title = (items[0].title.text if items[0].title else "")
        target_date = extract_date_from_title(first_title)
    if target_date:
        filtered_items = []
        for item in items:
            item_title = item.title.text if item.title else ""
            item_date = extract_date_from_title(item_title)
            if item_date and item_date.date() == target_date.date():
                filtered_items.append(item)
        items = filtered_items
        print(f"  Feed items selected: {len(items)} for {target_date.strftime('%Y-%m-%d')}")

    all_links = []
    fallback_fetches = 0
    new_items = 0

    for item in items:
        title = (item.title.text if item.title else "Untitled").strip()
        item_url = (item.link.text if item.link else "").strip()
        guid = (item.guid.text if item.guid else "").strip()
        item_key = guid or item_url or title
        existing_entry = source_log["scraped_articles"].get(item_key)

        if existing_entry and not should_reinspect_entry(existing_entry):
            continue

        description = item.description.text if item.description else ""
        encoded_node = item.find("content:encoded")
        encoded = encoded_node.text if encoded_node else ""

        links = extract_m3u_links(f"{description}\n{encoded}")
        extraction = "feed"

        if not links and item_url and fallback_fetches < 2:
            try:
                fallback_fetches += 1
                article_response = session.get(item_url, headers=HEADERS, timeout=20)
                links = extract_m3u_links(article_response.text)
                extraction = "article"
            except Exception as e:
                print(f"  Fallback fetch failed for {safe_title_for_log(title, 50)}: {e}")

        all_links.extend(links)
        write_source_log_entry(
            source_log=source_log,
            entry_key=item_key,
            title=title,
            url=item_url,
            links_count=len(links),
            extraction=extraction,
            existing_entry=existing_entry,
        )
        new_items += 1
        print(f"  -> {safe_title_for_log(title, 55)} | links={len(links)} via {extraction}")

    log_data["sources"][source_name] = source_log

    unique_links = len(dict.fromkeys(all_links))
    print(f"  New items processed: {new_items}")
    print(f"  Total links extracted: {len(all_links)} (unique={unique_links})")
    return all_links




def resolve_worker_count(cli_value, env_name, default_value, upper_bound=MAX_WORKERS_LIMIT):
    """Resolve worker count from CLI first, then env var, then default."""
    if cli_value is not None:
        value = cli_value
    else:
        raw = os.getenv(env_name)
        try:
            value = int(raw) if raw is not None else default_value
        except ValueError:
            value = default_value

    return max(1, min(value, upper_bound))


def parse_runtime_config():
    """Read runtime worker configuration."""
    parser = argparse.ArgumentParser(description="IPTV Playlist Updater")
    parser.add_argument("--validation-workers", type=int, default=None, help="Workers for existing playlist validation")
    parser.add_argument("--discovery-workers", type=int, default=None, help="Workers for discovery domain checks")
    args = parser.parse_args()
    return {
        "validation_workers": resolve_worker_count(
            args.validation_workers,
            "BLUNDER_VALIDATION_WORKERS",
            DEFAULT_VALIDATION_WORKERS,
        ),
        "discovery_workers": resolve_worker_count(
            args.discovery_workers,
            "BLUNDER_DISCOVERY_WORKERS",
            DEFAULT_DISCOVERY_WORKERS,
        ),
    }


def get_thread_session():
    """Create one requests session per worker thread."""
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = create_session()
        THREAD_LOCAL.session = session
    return session


def validate_existing_entry(item):
    """Worker for validating a single existing playlist entry."""
    slot_id = item.get("slot_id")
    url = item.get("url")
    domain = get_domain(url)
    name = item.get("name")
    started = time.perf_counter()
    is_valid, reason, stream_count = validate_playlist(url, get_thread_session())
    elapsed_sec = time.perf_counter() - started
    return {
        "slot_id": slot_id,
        "url": url,
        "domain": domain,
        "name": name,
        "item": item,
        "is_valid": is_valid,
        "reason": reason,
        "stream_count": stream_count,
        "elapsed_sec": elapsed_sec,
    }


def check_domain_links(domain, links):
    """Worker for finding the first valid playlist in a domain link set."""
    started = time.perf_counter()
    session = get_thread_session()

    checked_count = 0
    last_reason = "No links"
    for link in links:
        checked_count += 1
        is_valid, reason, stream_count = validate_playlist(link, session)
        last_reason = reason
        if is_valid:
            return {
                "domain": domain,
                "links_total": len(links),
                "checked_count": checked_count,
                "is_valid": True,
                "winning_link": link,
                "stream_count": stream_count,
                "last_reason": "Valid",
                "elapsed_sec": time.perf_counter() - started,
            }

    return {
        "domain": domain,
        "links_total": len(links),
        "checked_count": checked_count,
        "is_valid": False,
        "winning_link": None,
        "stream_count": 0,
        "last_reason": last_reason,
        "elapsed_sec": time.perf_counter() - started,
    }


def main():
    print("="*70)
    print("  IPTV PLAYLIST UPDATER")
    print("="*70)

    runtime = parse_runtime_config()
    print(
        f"[CONFIG] validation_workers={runtime['validation_workers']} | "
        f"discovery_workers={runtime['discovery_workers']}"
    )

    timestamp_now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {"featured_content": []}

    log_data = load_scraper_log()
    existing_slots = data.get("featured_content", [])

    print("\n" + "="*70)
    print("  VALIDATION PHASE - CHECKING ALL EXISTING PLAYLISTS")
    print("="*70)
    print(f"\n[VALIDATION] Checking {len(existing_slots)} existing playlists...")

    live_playlists = {}
    live_domains = set()
    available_slots = set()

    validation_started = time.perf_counter()
    validation_results = []
    if existing_slots:
        with ThreadPoolExecutor(max_workers=runtime["validation_workers"]) as executor:
            future_map = {
                executor.submit(validate_existing_entry, item): idx
                for idx, item in enumerate(existing_slots, 1)
            }
            for future in as_completed(future_map):
                idx = future_map[future]
                result = future.result()
                result["idx"] = idx
                validation_results.append(result)

        validation_results.sort(key=lambda x: x["idx"])

    for result in validation_results:
        idx = result["idx"]
        slot_id = result["slot_id"]
        url = result["url"]
        domain = result["domain"]
        name = result["name"]
        item = result["item"]
        is_valid = result["is_valid"]
        reason = result["reason"]
        stream_count = result["stream_count"]
        elapsed_sec = result["elapsed_sec"]

        url_preview = f"{url[:80]}..." if isinstance(url, str) else str(url)
        print(f"[{idx}/{len(existing_slots)}] TESTING: {name}")
        print(f"  Domain: {domain}")
        print(f"  URL: {url_preview}")

        if is_valid:
            item["channel_count"] = stream_count
            item["last_validated"] = timestamp_now
            item["logo_url"] = get_logo_for_slot(slot_id)
            live_playlists[slot_id] = item
            live_domains.add(domain)
            print(f"  [OK] ALIVE | Channels: {stream_count:,} | {elapsed_sec:.1f}s\n")
        else:
            available_slots.add(slot_id)
            print(f"  [FAIL] DEAD: {reason} | Slot {slot_id} cleared | {elapsed_sec:.1f}s\n")

    validation_elapsed = time.perf_counter() - validation_started

    print(f"\n[VALIDATION SUMMARY]")
    print(f"  Live Playlists: {len(live_playlists)}")
    print(f"  Dead Playlists: {len(available_slots)}")
    print(f"  Live Domains: {', '.join(sorted(live_domains))}")
    print(f"  Duration: {validation_elapsed:.1f}s")

    print("\n" + "="*70)
    print("  DISCOVERY PHASE - SCRAPING NEW PLAYLISTS")
    print("="*70)

    discovery_started = time.perf_counter()
    source_session = create_session()
    new_links = []
    new_links.extend(scrape_ninoiptv(source_session, log_data))
    new_links.extend(scrape_worldiptv(source_session, log_data))
    new_links.extend(scrape_iptvcodes(source_session, log_data))
    new_links.extend(scrape_m3umax(source_session, log_data))

    save_scraper_log(log_data)

    new_links = list(dict.fromkeys(new_links))
    print(f"\n[TESTING] Found {len(new_links)} unique new links")

    domain_links = {}
    for link in new_links:
        domain = get_domain(link)
        if domain not in domain_links:
            domain_links[domain] = []
        domain_links[domain].append(link)

    print(f"[TESTING] Grouped into {len(domain_links)} unique domains")

    added_count = 0
    skipped_domains = 0
    tested_domains = 0
    domains_to_test = []
    for domain, links in domain_links.items():
        if domain in live_domains:
            skipped_domains += 1
            print(f"\n[SKIP] Domain already live: {domain} ({len(links)} links skipped)")
            continue
        domains_to_test.append((domain, links))

    domain_results = {}
    if domains_to_test:
        with ThreadPoolExecutor(max_workers=runtime["discovery_workers"]) as executor:
            future_map = {
                executor.submit(check_domain_links, domain, links): (domain, links)
                for domain, links in domains_to_test
            }
            completed = 0
            total = len(domains_to_test)
            for future in as_completed(future_map):
                domain, links = future_map[future]
                completed += 1
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "domain": domain,
                        "links_total": len(links),
                        "checked_count": 0,
                        "is_valid": False,
                        "winning_link": None,
                        "stream_count": 0,
                        "last_reason": f"Worker error: {exc}",
                        "elapsed_sec": 0.0,
                    }
                domain_results[domain] = result
                status = "valid" if result["is_valid"] else "none"
                print(
                    f"[DISCOVERY PROGRESS] {completed}/{total} {domain}: {status} "
                    f"({result['checked_count']}/{result['links_total']} checked, {result['elapsed_sec']:.1f}s)"
                )

    for domain, links in domains_to_test:
        tested_domains += 1
        result = domain_results[domain]
        print(f"\n[NEW DOMAIN {tested_domains}] TESTING: {domain} ({len(links)} links)")

        if result["is_valid"]:
            link = result["winning_link"]
            stream_count = result["stream_count"]
            checked_count = result["checked_count"]

            if available_slots:
                target_id = min(available_slots)
                available_slots.remove(target_id)
                slot_type = "reused"
            else:
                target_id, slot_type = find_available_slot(set(live_playlists.keys()))

            name = get_name_for_slot(target_id, domain)
            logo = get_logo_for_slot(target_id)
            new_entry = {
                "slot_id": target_id,
                "name": name,
                "domain": domain,
                "channel_count": stream_count,
                "logo_url": logo,
                "type": "m3u",
                "url": link,
                "server_url": None,
                "id": f"slot_{str(target_id).zfill(3)}",
                "last_changed": timestamp_now,
                "last_validated": timestamp_now,
                "change_log": f"Added via scraper on {timestamp_now}",
            }

            live_playlists[target_id] = new_entry
            live_domains.add(domain)
            added_count += 1
            slot_info = f"Slot {target_id} ({slot_type})"
            print(
                f"  [OK] SUCCESS! Added to {slot_info} | Channels: {stream_count:,} "
                f"| checked {checked_count}/{len(links)} links"
            )
        else:
            print(
                f"  [FAIL] No valid playlist found "
                f"(checked {result['checked_count']}/{len(links)}): {result['last_reason']}"
            )

    renamed_count = rename_untitled_playlists(live_playlists)
    discovery_elapsed = time.perf_counter() - discovery_started

    print("\n" + "="*70)
    print("  FINALIZING")
    print("="*70)

    final_list = [live_playlists[sid] for sid in sorted(live_playlists.keys())]
    data["featured_content"] = final_list

    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    total_channels = sum(x['channel_count'] for x in final_list)

    print("\n" + "="*70)
    print("  SUMMARY")
    print("="*70)
    print(f"  Active Playlists: {len(final_list)}")
    print(f"  Newly Added: {added_count}")
    print(f"  Domains Tested: {tested_domains}")
    print(f"  Domains Skipped (Already Live): {skipped_domains}")
    if renamed_count > 0:
        print(f"  Renamed (Untitled -> Named): {renamed_count}")
    print(f"  Total Channels: {total_channels:,}")
    print(f"  Discovery Duration: {discovery_elapsed:.1f}s")
    print(f"  Updated: {timestamp_now}")
    print("="*70)


if __name__ == "__main__":
    main()
