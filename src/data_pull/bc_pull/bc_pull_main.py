from src.data_pull.bc_pull import bc_pull_transforms as tf
from src.util import _slack, sfdc


def main():
    sf_session = sfdc.SFDC()
    slack_session = _slack.Slack()

    # Extract
    download_url = "https://justice.gov.bc.ca/cannabislicensing/api/establishments/map-csv"
    df = tf.pull_df(download_url)

    bc_query = tf.format_bc_query()
    sf_df = sf_session.query_to_df(bc_query)

    parent_query = tf.format_parent_query()
    parent_df = sf_session.query_to_df(parent_query)

    bc_public_query = tf.format_bc_public_query()
    sf_public_df = sf_session.query_to_df(bc_public_query)

    # Transform
    df = tf.clean_up_df(df)
    df = tf.status_map(df)
    df, public_df = tf.split_df(df)
    status_df = tf.create_status_df(df, sf_df)

    # Load
    sf_session.bulk_upsert(status_df, "account", "Id")

    # Transform
    df = tf.find_new_stores(df, sf_df)
    parent_list = tf.get_parent_list(parent_df)
    df = tf.apply_get_parent(df, parent_list)
    df = tf.merge_parent_df(df, parent_df)
    df = tf.rename_columns(df)
    df = tf.broadcast_columns(df)

    if len(df) == 0:
        print("no new bc accounts")

    # Load
    sf_session.bulk_insert(df, 'account')

    # Alert
    if len(df) > 0:
        df = tf.apply_format_slack_message(df)
        message_list = tf.get_message_list(df)
        slack_session.stream_messages(
            message_list, channel='account-stream')
    else:
        slack_session.send_message(
            "no new BC accounts", channel='account-stream')

    # Transform
    public_df = tf.find_new_public_stores(public_df, sf_public_df)
    public_df = tf.rename_columns(public_df)
    public_df = tf.broadcast_public_columns(public_df)

    # Load
    sf_session.bulk_insert(public_df, 'account')

    # Alert
    if len(public_df) > 0:
        public_df = tf.apply_format_public_slack_message(public_df)
        message_list = tf.get_message_list(public_df)
        slack_session.stream_messages(
            message_list, channel='account-stream')
    else:
        slack_session.send_message(
            "no new *public* BC accounts", channel='account-stream')


if __name__ == "__main__":
    main()
