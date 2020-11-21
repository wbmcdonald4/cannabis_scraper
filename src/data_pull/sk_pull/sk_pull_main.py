from src.data_pull.sk_pull import sk_pull_transforms as tf
from src.util import _slack, sfdc


def main():
    
    sf_session = sfdc.SFDC()
    slack_session = _slack.Slack()

    # Extract
    sk_query = tf.format_sk_query()
    sf_df = sf_session.query_to_df(sk_query)
    
    parent_query = tf.format_parent_query()
    parent_df = sf_session.query_to_df(parent_query)
    
    url = "https://www.slga.com/permits-and-licences/cannabis-permits/cannabis-retailing/cannabis-retailers-in-saskatchewan"
    soup = tf.get_soup(url)

    tag_list = tf.pull_tag_list(soup)
    grouped_list, line_list = tf.split_into_groups(tag_list)

    list_1 = tf.pull_data_from_grouped_list(grouped_list)
    list_2 = tf.pull_data_from_line_list(line_list)

    df = tf.combine_lists(list_1, list_2)
    
    # Transform
    df = tf.clean_up_df(df)
    df = tf.find_new_stores(df, sf_df)
    parent_list = tf.get_parent_list(parent_df)
    df = tf.apply_get_parent(df, parent_list)
    df = tf.merge_parent_df(df, parent_df)
    tf.check_for_new_parents(df)
    df = tf.rename_columns(df)
    df = tf.broadcast_columns(df)
    
    # Load
    sf_session.bulk_insert(df, "account")

    # Alert
    if len(df) > 0:
        df = tf.apply_format_slack_message(df)
        message_list = tf.get_message_list(df)
        slack_session.stream_messages(message_list, channel='account-stream')
    else:
        slack_session.send_message(message='no new SK accounts', channel='sfdc_test_channel')

if __name__ == "__main__":
    main()