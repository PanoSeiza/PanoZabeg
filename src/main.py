from clickhouse_driver import Client
from datetime import datetimec
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pandas as pd
import os

CLICKHOUSE_HOST = 'ch0.prod.anal.panoramik.internal'

# переменные ниже надо будет установить следующими коммитами после того как будет сделано приложение в слаке
os.environ['SLACK_BOT_TOKEN'] = 'xoxb-495075105047-3664659326113-EaenSEVTgdIFfmFS52YME7g8'
client = WebClient(token=os.environ.get('SLACK_BOT_TOKEN'))
channel_id = 'C03JJRWNH7H' #канал panoramik-support

def send_message():
    try:
        # Пробуем отправить сообщение в слак
        client.chat_postMessage(
            channel=channel_id,
            text=send_tournament_report() #уточнить, как получить сообщения из этой функции
        )
    except SlackApiError as e:
        print(f'Error: {e}')


def get_from_ch(host, query):
    client = Client(host)
    data, columns = client.execute(query, with_column_types=True)
    columns = [column[0] for column in columns]
    dataframe = pd.DataFrame(data, columns=columns)
    return dataframe


def send_tournament_report():
    place_emoji = {
        0: '\U0001F947 ',
        1: '\U0001F948 ',
        2: '\U0001F949 ',
    }

    participants = {
        'профайл айди': 'имя игрока',
    }

    headers = {
        '2022-05-03': 'текст заголовка сообщения',
        '2022-05-04': 'текст заголовка сообщения',
    }

    query_pvp = (
        """
        SELECT
            profile_id,
            total_fights,
            fights_yesterday,
            fights_in_work_time,
            multiIf(league = '', '30', league) AS league
        FROM (
            SELECT
                *
            FROM (
                SELECT
                    arrayJoin(['{profile_ids}']) AS profile_id
            ) ALL LEFT JOIN (
                SELECT
                    attacker_id AS profile_id,
                    uniqExact(created_on) AS total_fights,
                    uniqExactIf(
                        created_on,
                        day = yesterday()
                    ) AS fights_yesterday,
                    uniqExactIf(
                        created_on,
                        day = yesterday()
                        AND day NOT IN ('2022-05-02', '2022-05-03', '2022-05-09', '2022-05-10')
                        AND toDayOfWeek(toDateTime(created_on)) IN (1, 2, 3, 4, 5)
                        AND toTime(toDateTime(created_on)) BETWEEN '1970-01-02 09:00:00' AND '1970-01-02 15:00:00'
                    ) AS fights_in_work_time
                FROM mp.pvp_stats_dist FINAL
                PREWHERE attacker_id IN ('{profile_ids}')
                AND match_type = 'pvp'
                AND day BETWEEN '2022-05-02' AND '2022-05-29'
                GROUP BY profile_id
            ) USING profile_id
        ) ALL LEFT JOIN (
            SELECT
                profile_id,
                argMax(
                    occurrence_value,
                    created_on
                ) AS league
            FROM mp.profile_dynamics_dist
            PREWHERE occurrence_type IN ('LeagueRaise', 'LeagueDrop')
            AND profile_id IN ('{profile_ids}')
            AND day BETWEEN '2022-05-02' and '2022-05-29'
            GROUP BY profile_id
        ) USING profile_id
        ORDER BY league, total_fights ASC
        """
    ).format(profile_ids="', '".join(list(participants.keys())))

    query_journey = (
        """
        SELECT
            profile_id,
            max(boss_overall) AS absolute_boss,
            argMax(island, boss_overall) AS island,
            argMax(sector, boss_overall) AS sector,
            argMax(boss, boss_overall) AS local_boss
        FROM mp.journey_progress_dist
        PREWHERE day BETWEEN '2022-05-02' AND '2022-05-29'
        AND profile_id IN ('{profile_ids}')
        GROUP BY profile_id
        ORDER BY
            absolute_boss DESC,
            island DESC,
            sector DESC,
            local_boss DESC
        limit 5;
        """
    ).format(profile_ids="', '".join(list(participants.keys())))

    df = get_from_ch(CLICKHOUSE_HOST, query_pvp)
    df_journey = get_from_ch(CLICKHOUSE_HOST, query_journey)

    today_date = datetime.utcnow().date()
    body_message = (
        f'#PANO_Забег\n'
        f'{headers[str(today_date)]}'
        f'<u>ПВП:</u>\n'
    )
    body_footer = '\n(всего боев / бои вчера / лига)'

    journey_message = '\n\n<u>ДЖОРНИ:</u>\n'
    journey_footer = '\n(остров / сектор / текущий босс / всего побеждено боссов)'

    footer_message = ''
    footer_header = '\n\n\nРебята, вы шо играете на работе? Список пупсиков, которым надо выдать по жопе:\n\n'
    footer_footer = '\n(бои вместо работы вчера)'

    for idx, row in df.iterrows():
        participant = f'{place_emoji.get(idx, "")}{participants[row.profile_id]}'

        body_message += (
            f'<b>{idx + 1}. {participant}</b> - {row.total_fights}'
            f' / {row.fights_yesterday}'
            f' / <u>{row.league if row.league else 30}</u>\n'
        )

        if row.fights_in_work_time:
            footer_message += (
                f'{participants[row.profile_id]}'
                f' - {row.fights_in_work_time}\n'
            )
    body_message += body_footer

    for idx, row in df_journey.iterrows():
        participant = f'{place_emoji.get(idx, "")}{participants[row.profile_id]}'

        journey_message += (
            f'<b>{idx + 1}. {participant}</b> - {int(row.island) + 1}'
            f' / {int(row.sector) + 1}'
            f' / {int(row.local_boss) + 1}'
            f' / <u>{int(row.absolute_boss) + 1}</u>\n'
        )
    journey_message += journey_footer
    body_message += journey_message

    if footer_message:
        body_message += f'{footer_header}{footer_message}{footer_footer}'

    # body_message - сообщение, которые нужно отправить
    # тут отправка сообщения в слак
