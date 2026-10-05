FROM aiogram/telegram-bot-api:latest

USER root

RUN apk add --no-cache python3

COPY start.sh /start.sh

RUN chmod +x /start.sh

ENTRYPOINT ["/start.sh"]
