url = 'http://www.qqzhi.com/uploadpic/2015-02-18/010316721.jpg'

import requests

r = requests.get(url,stream=True)

if r.status_code == 200:
    files = {
        'file' : ('123123.jpg',r.content,'image/jpg')
    }

    rr = requests.post('http://127.0.0.1:5000/upload',files=files)
    if rr.status_code == 200:
        print 200