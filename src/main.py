# from clickhouse_driver import Client
import clickhouse_connect
from datetime import datetime
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pandas as pd
import os

# CLICKHOUSE_HOST = 'ch0.prod.anal.panoramik.internal'
CLICKHOUSE_HOST = 'ch.prod.playful-fairies.com'
os.environ['SLACK_BOT_TOKEN'] = 'xoxb-495075105047-3664659326113-BZpMqiXPbpWzMiEex5zuVtqq'
client = WebClient(token=os.environ.get('SLACK_BOT_TOKEN'))

channel_id = 'C03K08F2SCA'  # test channel
# channel_id = 'C03JJRWNH7H'  # product support


def test_message():
    try:
        client.chat_postMessage(
            channel='C03K08F2SCA',
            text="ohayo, message time!"
        )
    except SlackApiError as e:
        print(f'Error: {e}')


def send_message():
    try:
        client.chat_postMessage(
            channel=channel_id,
            text=send_tournament_report()  # теперь просто получаем
        )
    except SlackApiError as e:
        print(f'Error: {e}')


def get_from_ch(host, query):
    # client = Client(host=host, port='443')
    client_web = clickhouse_connect.get_client(host=host, port=443)
    # data, columns = client.execute(query, with_column_types=True)
    res = client_web.query(query)
    data = res.result_set
    # print(data)
    columns = res.column_names

    # columns = [column[0] for column in columns]
    dataframe = pd.DataFrame(data, columns=columns)
    return dataframe


def send_tournament_report():
    start_end = ['2022-06-09', '2022-06-16']

    place_emoji = {
        0: '\U0001F947 ',
        1: '\U0001F948 ',
        2: '\U0001F949 ',
    }

    participants = {
        'WEBGLGUEST12bfe7c60a8001cbe6e9f2d6fd07741a': 'RobinGood',
        'APPLEdb7795d54d11cf0d5296cc3a473a0eb3': 'Sashiaymo',
        'AND697c2a78d62dc7b20826763036b1b750': 'kakoito loh',
    }

    participants_names = {
        ''
    }

    work_time = {
        'WEBGLGUEST12bfe7c60a8001cbe6e9f2d6fd07741a': {'from': '1970-01-02 09:00:00', 'to': '1970-01-02 19:00:00'},
        'APPLEdb7795d54d11cf0d5296cc3a473a0eb3': {'from': '1970-01-02 09:00:00', 'to': '1970-01-02 19:00:00'},
        'AND697c2a78d62dc7b20826763036b1b750': {'from': '1970-01-02 09:00:00', 'to': '1970-01-02 19:00:00'},
    }

    for k in participants.keys():
        if k not in work_time.keys():
            work_time[k] = {'from': '1970-01-02 00:00:00', 'to': '1970-01-02 00:00:00'}

    work_time_str = str(work_time).replace("'", '"')

    headers = {
        '2022-06-10': 'Шел первый день как я пытался продолбиться в базы данных...',
        '2022-06-11': 'Шел второй день как я пытался продолбиться в базы данных...',
        '2022-06-12': 'Шел третий день как я пытался продолбиться в базы данных...',
        '2022-06-13': 'Шел четвертый день как я пытался продолбиться в базы данных...',
        '2022-06-14': 'Шел пятый день как я пытался продолбиться в базы данных...',
        '2022-06-15': 'Шел шестой день как я пытался продолбиться в базы данных...',
        '2022-06-16': 'Шел седьмой день как я пытался продолбиться в базы данных...',
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
                        AND (
                            toTime(toDateTime(created_on)) 
                            BETWEEN toTime(toDateTime(JSONExtractString('{work_time_str}', attacker_id, 'from'))) 
                            AND toTime(toDateTime(JSONExtractString('{work_time_str}', attacker_id, 'to')))
                        )
                    ) AS fights_in_work_time
                FROM mp.pvp_stats_dist FINAL
                PREWHERE attacker_id IN ('{profile_ids}')
                AND match_type = 'pvp'
                AND day BETWEEN '{start}' AND '{end}'
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
            AND day BETWEEN '{start}' and '{end}'
            GROUP BY profile_id
        ) USING profile_id
        ORDER BY league, total_fights ASC
        """
    ).format(profile_ids="', '".join(list(participants.keys())), work_time_str=work_time_str, start=start_end[0], end=start_end[1])

    query_journey = (
        """
        SELECT
            profile_id,
            max(boss_overall) AS absolute_boss,
            argMax(island, boss_overall) AS island,
            argMax(sector, boss_overall) AS sector,
            argMax(boss, boss_overall) AS local_boss
        FROM mp.journey_progress_dist
        PREWHERE day BETWEEN '{start}' AND '{end}'
        AND profile_id IN ('{profile_ids}')
        GROUP BY profile_id
        ORDER BY
            absolute_boss DESC,
            island DESC,
            sector DESC,
            local_boss DESC
        limit 5
        """
    ).format(profile_ids="', '".join(list(participants.keys())), start=start_end[0], end=start_end[1])

    df = get_from_ch(CLICKHOUSE_HOST, query_pvp)
    df_journey = get_from_ch(CLICKHOUSE_HOST, query_journey)

    today_date = datetime.utcnow().date()
    body_message = (
        f'#PANO_Забег _{today_date}_\n'
        f'{headers[str(today_date)]} \n\n'
        f'_ПВП:_\n'
    )
    body_footer = '\n(всего боев / бои вчера / лига)'

    journey_message = '\n\n_ДЖОРНИ:_\n'
    journey_footer = '\n(остров / сектор / текущий босс / всего побеждено боссов)'

    footer_message = ''
    footer_header = '\n\n\nРебята, вы шо играете на работе? Список пупсиков, которым надо выдать по жопе:\n\n'
    footer_footer = '\n(бои вместо работы вчера)'

    print(df)
    for idx, row in df.iterrows():
        # print(row)
        participant = f'{place_emoji.get(idx, "")}{participants[row.profile_id]}'

        body_message += (
            f'*{idx + 1}. {participant}* - {row.total_fights}'
            f' / {row.fights_yesterday}'
            f' / _{row.league if row.league else 30}_\n'
        )

        if row.fights_in_work_time:
            footer_message += (
                f'*{participants[row.profile_id]}*'
                f' - {row.fights_in_work_time}\n'
            )
    body_message += body_footer

    for idx, row in df_journey.iterrows():
        participant = f'{place_emoji.get(idx, "")}{participants[row.profile_id]}'

        journey_message += (
            f'*{idx + 1}. {participant}* - {int(row.island) + 1}'
            f' / {int(row.sector) + 1}'
            f' / {int(row.local_boss) + 1}'
            f' / _{int(row.absolute_boss) + 1}_\n'
        )
    journey_message += journey_footer
    body_message += journey_message

    if footer_message:
        body_message += f'{footer_header}{footer_message}{footer_footer}'

    return body_message


# while True:
#     time.sleep(1)
#     z = time.localtime()
#     if z.tm_hour == 19 and z.tm_min == 0 and z.tm_sec == 0:
#         test_message()
#         send_message()


test_message()
send_message()
