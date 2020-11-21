from src.data_pull.ab_pull import ab_pull_transforms as tf
from src.util import _slack, sfdc


def main():
    sf_session = sfdc.SFDC()
    slack_session = _slack.Slack()

    # Extract
    download_url = "https://aglc.ca/cannabis/cannabis-licensee-report/EXCEL"

    df = tf.pull_df(download_url)

    ab_query = tf.format_ab_query()
    sf_df = sf_session.query_to_df(ab_query)

    parent_query = tf.format_parent_query()
    parent_df = sf_session.query_to_df(parent_query)

    # Transform
    df = tf.clean_up_df(df)
    df = tf.find_new_stores(df, sf_df)
    parent_list = tf.get_parent_list(parent_df)
    df = tf.apply_get_parent(df, parent_list)
    df = tf.merge_parent_df(df, parent_df)
    df = tf.rename_columns(df)
    df = tf.broadcast_columns(df)

    # Load
    sf_session.bulk_insert(df, 'account')

    # Alert
    if len(df) > 0:
        df = tf.apply_format_slack_message(df)
        message_list = tf.get_message_list(df)
        slack_session.stream_messages(message_list, channel='account-stream')
    else:
        slack_session.send_message(message='no new AB accounts', channel='sfdc_test_channel')

if __name__ == "__main__":
    main()


