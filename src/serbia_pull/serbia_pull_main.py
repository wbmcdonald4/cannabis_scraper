from src.serbia_pull import serbia_pull_transforms as tf
from src.util import _slack, google_sheets, sfdc


def main():
    gs_session = google_sheets.GoogleSheets()
    sf_session = sfdc.SFDC()
    slack_session = _slack.Slack()

    spreadsheet_id = '1R3kpjcKlq9J6Df32PGC1RZXVYBjlgRRz9OcV91e4cp8'
    sheet_name = tf.get_last_monday()
    sheet_range = "A1:ZZ"

    gs_values = gs_session.get_google_sheet(spreadsheet_id, sheet_name, sheet_range)
    df = tf.convert_gsheets_values_to_df(gs_values)

    df = tf.adjustments_on_import(df)
    
    sf_account_query = tf.format_sf_account_query()
    sf_account_df = sf_session.query_to_df(sf_account_query)
    
    df = tf.quick_sf_acc_check(df, sf_account_df)
    update_df = tf.create_update_df(df)
    df = tf.clean_up_df(df)
    df = tf.melt_df(df)
    df = tf.broadcast_columns(df, sheet_name)
    check_in_df = tf.create_check_in_df(df)
    
    sf_product_query = tf.format_sf_product_query()
    product_df = sf_session.query_to_df(sf_product_query)
    df = tf.merge_product_df(df, product_df)
    
    sf_session.bulk_upsert(update_df, "account", "Id")
    sf_session.bulk_insert(check_in_df, "store_check_in")
    
    sf_sc_query = tf.format_sf_sc_query(sheet_name)
    sc_df = sf_session.query_to_df(sf_sc_query)
    
    check_in_item_df = tf.merge_sc_df(df, sc_df)
    sf_session.bulk_insert(check_in_item_df, "check_in_item")

    slack_session.send_message(message=':flag-rs: serbia distribution upload complete', channel='account-stream')
    
    
if __name__ == "__main__":
    main()
