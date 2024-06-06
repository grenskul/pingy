FROM python:3

ENV discord_token ${discord_token}

ADD bot.py /

ADD requirements.txt /

RUN pip install -r requirements.txt

CMD python3 bot.py
