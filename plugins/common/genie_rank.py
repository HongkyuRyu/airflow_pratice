def genie_rank_df():
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    from datetime import datetime, timedelta

    url = 'https://www.genie.co.kr/chart/top200?ditc=D&rtm=N'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    columns = ['rank', 'title', 'artist', 'album', 'diff_rank']
    data = []

    for song_entry in soup.select('tr.list'):
        rank_text = song_entry.select_one('.number').text.strip()
        rank = int(rank_text.split()[0])  # 순위

        # 순위 차이
        diff_rank = None
        if len(rank_text.split()) > 1:
            diff_rank = rank_text.split()[1]

        title = song_entry.select_one('a.title').text.strip()
        artist = song_entry.select_one('a.artist').text.strip()
        album = song_entry.select_one('a.albumtitle').text.strip()

        data.append({
            'rank': rank,
            'title': title,
            'artist': artist,
            'album': album,
            'diff_rank': diff_rank
        })

    df = pd.DataFrame(data, columns=columns)

    # 어제 순위 계산
    yesterday_date = datetime.now() - timedelta(days=1)
    yesterday_column_name = yesterday_date.strftime('%Y%m%d')

    def calculate(row):
        if row['diff_rank'] == '유지':
            return row['rank']
        elif '상승' in row['diff_rank']:
            return row['rank'] + int(row['diff_rank'][:-2])
        elif '하강' in row['diff_rank']:
            return row['rank'] - int(row['diff_rank'][:-2])
        else:  # 신곡
            return "new"

    df[yesterday_column_name] = df.apply(calculate, axis=1)

    return df
