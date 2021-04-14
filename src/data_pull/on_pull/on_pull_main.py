from src.data_pull.on_pull import on_pull_transforms as tf
from src.util import _slack, sfdc


def main():
    
    sf_session = sfdc.SFDC()
    slack_session = _slack.Slack()

    # Extract
    base_url = 'https://www.agco.ca/status-current-cannabis-retail-store-applications'
    
    num_pages = tf.get_num_pages(base_url)
    df = tf.scrape_website(num_pages, base_url)
    df = tf.clean_up_df(df)
    df = tf.status_map(df)
    df = tf.status_filter(df)
    print(df.shape)

    on_query = tf.format_on_query()
    sf_df = sf_session.query_to_df(on_query)

    df = tf.find_new_stores(df, sf_df)
    parent_query = tf.format_parent_query()
    parent_df = sf_session.query_to_df(parent_query)
    parent_list = tf.get_parent_list(parent_df)
    df = tf.apply_get_parent(df, parent_list)
    df = tf.merge_parent_df(df, parent_df)
    tf.check_for_new_parents(df)
    df = tf.rename_columns(df)
    df = tf.broadcast_columns(df)
    
    # Load Accounts
    sf_session.bulk_insert(df, "account")
    
    # Alert
    if len(df) > 0:
        df = tf.apply_format_slack_message(df)
        message_list = tf.get_message_list(df)
        slack_session.stream_messages(message_list, channel='account-stream')
    else:
        slack_session.send_message(message='no new ON accounts', channel='sfdc_test_channel')
    
if __name__ == "__main__":
    main()