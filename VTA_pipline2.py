# import necessary materials
import sqlite3
import pandas as pd
import glob
import os
from tqdm import tqdm
import datetime
from datetime import timedelta
pd.options.mode.chained_assignment = None

# create connection to sqlite
conn = sqlite3.connect(r"D:\NMT\OneDrive\Viettravel Airline\Database\VTA_RM_test.db")
c = conn.cursor()
warining_msg = '*****WARNING***** cannot add new rows to SQLite'

# create delete rows functions----------------------------------------------------------------

def delete_rows_SQL(table, file_mame):
    query = f"""
        DELETE FROM {table}
            WHERE file_name = '{file_mame}'
        """
    c.execute(query)
    conn.commit()

def delete_rows_cmd(row):
    delete_rows_SQL(row['table_name'], row['file_name'])
    return None

def apply_delete_row(table:pd.DataFrame):
    if table.shape[0] != 0:
        table.apply(delete_rows_cmd, axis=1)
        print('|_________ rows deleted')
    return None
    
# _______________ fact tables _______________

def file_name_modified(path:str):
    all_dir_files = glob.glob(path + '/*')
    m_time = []

    for dir_file in all_dir_files:

        # file modification timestamp of a file
        f_m_time = os.path.getmtime(dir_file)
        # convert timestamp into DateTime object
        f_dt_m = datetime.datetime.fromtimestamp(f_m_time)
        m_time += [f_dt_m]

    files_name = pd.Series(all_dir_files).str.split('\\', expand=True).iloc[:, -1]
    table_name = pd.Series(all_dir_files).str.split('\\', expand=True).iloc[:, -2]
    return pd.DataFrame({'file_name':files_name, 'modified_time':m_time,
                         'table_name':table_name, 'dir_file':all_dir_files})
# ---------------------------------------------------------------------------------

def load_payment_detail():
    payment_detail_files_dir = add_table[add_table['table_name'] == 'payment_detail'].reset_index(drop=True)['dir_file']
    payment_detail_files_mod_time = add_table[add_table['table_name'] == 'payment_detail'].reset_index(drop=True)['modified_time']
    payment_detail_files_name = add_table[add_table['table_name'] == 'payment_detail'].reset_index(drop=True)['file_name']

    if len(payment_detail_files_dir) != 0:

        # read files into a dataframe
        li = []
        for i in tqdm(range(len(payment_detail_files_dir)), desc='load payment_detail'):
            df = pd.read_csv(payment_detail_files_dir[i], skiprows=2)
            df.dropna(how='all', inplace=True)
            df['file_name'] = payment_detail_files_name[i]
            df['modified_time'] = payment_detail_files_mod_time[i]
            li.append(df)
        add_payment_detail = pd.concat(li, axis=0, ignore_index=True)

        #     drop the 'unamed 39' column if exists
        if len(add_payment_detail.columns) == 42:
            add_payment_detail.drop(add_payment_detail.columns[-3], axis=1, inplace=True)

    #         rename columns
        add_payment_detail.columns = ['CONFIRMATION_NUM', 'BOOKING_TYPE', 'RES_STATUS',
           'RES_PAYMENT_ID', 'BOOK_DATE_GMT', 'BOOK_DATE_LCL', 'LAST_MODIFIED_GMT',
           'LAST_MODIFIED_LCL', 'BOOKING_AGENT', 'CRS_CODE', 'USER_ID',
           'DEPT_NAME', 'DESCRIPTION', 'IATA_NUM', 'PARENT_IATA_NUM',
           'PERSON_ORG_ID', 'FIRST_NAME', 'LAST_NAME', 'CARDHOLDER_NAME',
           'DATE_PAID_GMT', 'DATE_PAID_LCL', 'ACCOUNT_NUM', 'PAYMENT_TYPE',
           'PAYMENT_METHOD', 'PAYMENT_METHOD_DESC', 'TRANS_STATUS', 'REFERENCE',
           'AUTHORIZATION_CODE', 'VOUCHER_NUM', 'VOUCHER_NUM_FULL',
           'VCHR_CREATED_GMT', 'VCHR_CREATED_LCL', 'RES_CURRENCY', 'CURRENCY_PAID',
           'AMOUNT_PAID', 'BASE_CURRENCY', 'BASE_AMOUNT', 'RPT_CURRENCY',
           'RPT_AMOUNT', 'file_name', 'modified_time']
        add_payment_detail['IATA_NUM'].fillna(add_payment_detail['USER_ID'], inplace=True)
        add_payment_detail['IATA_NUM'].fillna(add_payment_detail['BOOKING_AGENT'], inplace=True)
        try:
            tmp = add_payment_detail['RPT_AMOUNT'].copy()
            add_payment_detail['RPT_AMOUNT'] = add_payment_detail['RPT_AMOUNT'].str.replace(',', '')
            add_payment_detail['RPT_AMOUNT'].fillna(tmp, inplace=True)
        except:
            pass

    #     delete and update new rows to SQLite
        apply_delete_row(remove_table[remove_table['table_name'] == 'payment_detail'])
        try: add_payment_detail.to_sql('payment_detail', conn, if_exists='append', index=False)
        except: print(warining_msg)
    else:
        print('No "Payment details" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'payment_detail'])

# ---------------------------------------------------------------------------------

def load_pax_revenue():
    pax_revenue_files_dir = add_table[add_table['table_name'] == 'pax_revenue'].reset_index(drop=True)['dir_file']
    pax_revenue_files_mod_time = add_table[add_table['table_name'] == 'pax_revenue'].reset_index(drop=True)['modified_time']
    pax_revenue_files_name = add_table[add_table['table_name'] == 'pax_revenue'].reset_index(drop=True)['file_name']

    if len(pax_revenue_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(pax_revenue_files_dir)), desc='load pax_revenue'):
            df = pd.read_excel(pax_revenue_files_dir[i], index_col=None, header=0)
            df['file_name'] = pax_revenue_files_name[i]
            df['modified_time'] = pax_revenue_files_mod_time[i]
            # df.columns = ['CONFIRMATION_NUM', 'RES_SEG_STATUS_DESCRIPTION', 'BOOKING_AGENT', 'IATA_NUM', 'FARE_CLASS_CODE'
            #              , 'SAVED_FB_CODE', 'PTC_DESCRIPTION', 'TITLE', 'LAST_NAME', 'FIRST_NAME', 'INFANT', 'FLIGHT_NUM'
            #              , 'CARRIER_CODE', 'FROM_AIRPORT', 'TO_AIRPORT', 'DEPARTURE_DATE', 'DEPARTURE_TIME'
            #              , 'FLIGHT_STATUS', 'BOOK_DATE', 'CNT_DATE', 'BF', 'PNLT', 'AX', 'C4', 'YR01', 'YR02'
            #              , 'YQ', 'TOTAL_TAX', 'SERVICES_FEE', 'file_name', 'modified_time']
            li.append(df)
        add_pax_revenue = pd.concat(li, axis=0, ignore_index=True)

    #   delete and update new rows to SQLite
        apply_delete_row(remove_table[remove_table['table_name'] == 'pax_revenue'])
        try: add_pax_revenue.to_sql('pax_revenue', conn, if_exists='append', index=False)
        except: print(warining_msg)
    else:
        print('No "Pax Revenue" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'pax_revenue'])

# ---------------------------------------------------------------------------------

def load_inflow_cash():
    inflow_cash_files_dir = add_table[add_table['table_name'] == 'inflow_cash'].reset_index(drop=True)['dir_file']
    inflow_cash_files_mod_time = add_table[add_table['table_name'] == 'inflow_cash'].reset_index(drop=True)['modified_time']
    inflow_cash_files_name = add_table[add_table['table_name'] == 'inflow_cash'].reset_index(drop=True)['file_name']

    if len(inflow_cash_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(inflow_cash_files_dir)), desc='load inflow_cash'):
            df = pd.read_excel(inflow_cash_files_dir[i], index_col=None, header=0)
            df['file_name'] = inflow_cash_files_name[i]
            df['modified_time'] = inflow_cash_files_mod_time[i]
            li.append(df)
        add_inflow_cash = pd.concat(li, axis=0, ignore_index=True)

    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'inflow_cash'])
        try: add_inflow_cash.to_sql('inflow_cash', conn, if_exists='append', index=False)
        except: print(warining_msg)

    else: 
        print('No "Inflow Cash" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'inflow_cash'])
# ---------------------------------------------------------------------------------

def load_cargo():
    cargo_files_dir = add_table[add_table['table_name'] == 'cargo'].reset_index(drop=True)['dir_file']
    cargo_files_mod_time = add_table[add_table['table_name'] == 'cargo'].reset_index(drop=True)['modified_time']
    cargo_files_name = add_table[add_table['table_name'] == 'cargo'].reset_index(drop=True)['file_name']

    if len(cargo_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(cargo_files_dir)), desc='load cargo files'):
            df = pd.read_excel(cargo_files_dir[i], index_col=None, header=0)
            df['file_name'] = cargo_files_name[i]
            df['modified_time'] = cargo_files_mod_time[i]
            li.append(df)
        add_cargo = pd.concat(li, axis=0, ignore_index=True)

    #     rename column
        add_cargo.columns = ['flight_date', 'flight_no', 'routing', 'cargo_cap', 'cargo_load_factor', 'awbs',
                        'gross_wt', 'charge_wt', 'cbm', 'revenue_before_tax',
                        'revenue_after_tax', 'file_name', 'modified_time']

    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'cargo'])
        try: add_cargo.to_sql('cargo', conn, if_exists='append', index=False)
        except: print(warining_msg)

    else: 
        print('No "cargo" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'cargo'])

# ---------------------------------------------------------------------------------

def load_flown_aircraft_leg():
    flown_aircraft_leg_files_dir = add_table[add_table['table_name'] == 'flown_aircraft_leg'].reset_index(drop=True)['dir_file']
    flown_aircraft_leg_files_mod_time = add_table[add_table['table_name'] == 'flown_aircraft_leg'].reset_index(drop=True)['modified_time']
    flown_aircraft_leg_files_name = add_table[add_table['table_name'] == 'flown_aircraft_leg'].reset_index(drop=True)['file_name']

    if len(flown_aircraft_leg_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(flown_aircraft_leg_files_dir)), desc='load flown_aircraft_leg files'):
            df = pd.read_excel(flown_aircraft_leg_files_dir[i], index_col=None, header=0)
            df['file_name'] = flown_aircraft_leg_files_name[i]
            df['modified_time'] = flown_aircraft_leg_files_mod_time[i]
            li.append(df)
        add_flown_aircraft_leg = pd.concat(li, axis=0, ignore_index=True)

    #     rename column
        add_flown_aircraft_leg.columns = ['carrier', 'flt_num', 'suffix', 'Flight_Origin_Date_LT',
           'dep_station', 'arr_station', 'std_lt', 'sta_lt', 'aircraft_type',
           'config', 'flight_status', 'traffic_restriction_code',
           'schedule_group', 'file_name', 'modified_time']

    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'flown_aircraft_leg'])
        try: add_flown_aircraft_leg.to_sql('flown_aircraft_leg', conn, if_exists='append', index=False)
        except: print(warining_msg)

    else:
        print('No "flown_aircraft_leg" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'flown_aircraft_leg'])
# ---------------------------------------------------------------------------------        

def load_reservation():
    reservation_files_dir = add_table[add_table['table_name'] == 'reservation'].reset_index(drop=True)['dir_file']
    reservation_files_mod_time = add_table[add_table['table_name'] == 'reservation'].reset_index(drop=True)['modified_time']
    reservation_files_name = add_table[add_table['table_name'] == 'reservation'].reset_index(drop=True)['file_name']

    if len(reservation_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(reservation_files_dir)), desc='load reservation'):
            df = pd.read_excel(reservation_files_dir[i], index_col=None, header=0)
            df['file_name'] = reservation_files_name[i]
            df['modified_time'] = reservation_files_mod_time[i]
            li.append(df)
        add_reservation = pd.concat(li, axis=0, ignore_index=True)

    #     transform
        add_reservation = add_reservation[~(add_reservation['Status'] == 'Canceled')]
        add_reservation['Book Date'] = pd.to_datetime(add_reservation['Book Date'])
        add_reservation['Departure Date'] = pd.to_datetime(add_reservation['Departure Date'])
        add_reservation.columns = ['last_name', 'first_name', 'confirm', 'book_date', 'departure_date', 
                              'flight_number', 'class_code', 'ori_station', 'des_station', 'status', 'iata',
                              'copr_id', 'record_locator', 'eticket_num', 'file_name', 'modified_time']

    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'reservation'])
        try: add_reservation.to_sql('reservation', conn, if_exists='append', index=False)
        except: print(warining_msg)

    else:
        print('No "Pax Revenue" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'reservation'])
                
# _______________ dim tables_______________

def load_dim_agent():
    dim_agent_files_dir = add_table[add_table['table_name'] == 'dim_agent'].reset_index(drop=True)['dir_file']
    dim_agent_files_mod_time = add_table[add_table['table_name'] == 'dim_agent'].reset_index(drop=True)['modified_time']
    dim_agent_files_name = add_table[add_table['table_name'] == 'dim_agent'].reset_index(drop=True)['file_name']

    if len(dim_agent_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(dim_agent_files_dir)), desc='load dim_agent files'):
            df = pd.read_excel(dim_agent_files_dir[i], index_col=None, header=0)
            df['file_name'] = dim_agent_files_name[i]
            df['modified_time'] = dim_agent_files_mod_time[i]
            li.append(df)
        add_dim_agent = pd.concat(li, axis=0, ignore_index=True)


    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_agent'])
        try: add_dim_agent.to_sql('dim_agent', conn, if_exists='append', index=False)
        except: print(warining_msg)

    else:
        print('No "dim_agent" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_agent'])
# ---------------------------------------------------------------------------------

def load_dim_calendar():
    dim_calendar_files_dir = add_table[add_table['table_name'] == 'dim_calendar'].reset_index(drop=True)['dir_file']
    dim_calendar_files_mod_time = add_table[add_table['table_name'] == 'dim_calendar'].reset_index(drop=True)['modified_time']
    dim_calendar_files_name = add_table[add_table['table_name'] == 'dim_calendar'].reset_index(drop=True)['file_name']

    if len(dim_calendar_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(dim_calendar_files_dir)), desc='load dim_calendar files'):
            df = pd.read_excel(dim_calendar_files_dir[i], index_col=None, header=0)
            df['file_name'] = dim_calendar_files_name[i]
            df['modified_time'] = dim_calendar_files_mod_time[i]
            li.append(df)
        add_dim_calendar = pd.concat(li, axis=0, ignore_index=True)


    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_calendar'])
        try: add_dim_calendar.to_sql('dim_calendar', conn, if_exists='append', index=False)
        except: print(warining_msg)

    else:
        print('No "dim_calendar" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_calendar'])
# ---------------------------------------------------------------------------------

def load_dim_fare_code():
    dim_fare_code_files_dir = add_table[add_table['table_name'] == 'dim_fare_code'].reset_index(drop=True)['dir_file']
    dim_fare_code_files_mod_time = add_table[add_table['table_name'] == 'dim_fare_code'].reset_index(drop=True)['modified_time']
    dim_fare_code_files_name = add_table[add_table['table_name'] == 'dim_fare_code'].reset_index(drop=True)['file_name']

    if len(dim_fare_code_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(dim_fare_code_files_dir)), desc='load dim_fare_code files'):
            df = pd.read_excel(dim_fare_code_files_dir[i], index_col=None, header=0)
            df['file_name'] = dim_fare_code_files_name[i]
            df['modified_time'] = dim_fare_code_files_mod_time[i]
            li.append(df)
        add_dim_fare_code = pd.concat(li, axis=0, ignore_index=True)


    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_fare_code'])
        try: add_dim_fare_code.to_sql('dim_fare_code', conn, if_exists='append', index=False)
        except: print(warining_msg)

    else:
        print('No "dim_fare_code" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_fare_code'])
# ---------------------------------------------------------------------------------        

def load_dim_routes():
    dim_routes_files_dir = add_table[add_table['table_name'] == 'dim_routes'].reset_index(drop=True)['dir_file']
    dim_routes_files_mod_time = add_table[add_table['table_name'] == 'dim_routes'].reset_index(drop=True)['modified_time']
    dim_routes_files_name = add_table[add_table['table_name'] == 'dim_routes'].reset_index(drop=True)['file_name']

    if len(dim_routes_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(dim_routes_files_dir)), desc='load dim_routes files'):
            df = pd.read_excel(dim_routes_files_dir[i], index_col=None, header=0)
            df['file_name'] = dim_routes_files_name[i]
            df['modified_time'] = dim_routes_files_mod_time[i]
            li.append(df)
        add_dim_routes = pd.concat(li, axis=0, ignore_index=True)


    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_routes'])
        try: add_dim_routes.to_sql('dim_routes', conn, if_exists='append', index=False)
        except: print(warining_msg)
        
    else:
        print('No "dim_routes" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_routes'])
# ---------------------------------------------------------------------------------              
        
def load_dim_slot_time():
    dim_slot_time_files_dir = add_table[add_table['table_name'] == 'dim_slot_time'].reset_index(drop=True)['dir_file']
    dim_slot_time_files_mod_time = add_table[add_table['table_name'] == 'dim_slot_time'].reset_index(drop=True)['modified_time']
    dim_slot_time_files_name = add_table[add_table['table_name'] == 'dim_slot_time'].reset_index(drop=True)['file_name']

    if len(dim_slot_time_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(dim_slot_time_files_dir)), desc='load dim_slot_time files'):
            df = pd.read_excel(dim_slot_time_files_dir[i], index_col=None, header=0)
            df['file_name'] = dim_slot_time_files_name[i]
            df['modified_time'] = dim_slot_time_files_mod_time[i]
            li.append(df)
        add_dim_slot_time = pd.concat(li, axis=0, ignore_index=True)


    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_slot_time'])
        try: add_dim_slot_time.to_sql('dim_slot_time', conn, if_exists='replace', index=False)
        except: print(warining_msg)

    else:
        print('No "dim_slot_time" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_slot_time'])
# ---------------------------------------------------------------------------------              
        
def load_flight_type():
    flight_type_files_dir = add_table[add_table['table_name'] == 'flight_type'].reset_index(drop=True)['dir_file']
    flight_type_files_mod_time = add_table[add_table['table_name'] == 'flight_type'].reset_index(drop=True)['modified_time']
    flight_type_files_name = add_table[add_table['table_name'] == 'flight_type'].reset_index(drop=True)['file_name']

    if len(flight_type_files_dir) != 0:
    #     read files into a dataframe
        li = []
        for i in tqdm(range(len(flight_type_files_dir)), desc='load flight_type files'):
            df = pd.read_excel(flight_type_files_dir[i], index_col=None, header=0)
            df['file_name'] = flight_type_files_name[i]
            df['modified_time'] = flight_type_files_mod_time[i]
            li.append(df)
        add_flight_type = pd.concat(li, axis=0, ignore_index=True)


    #     load to sqlite
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_slot_time'])
        try: add_flight_type.to_sql('flight_type', conn, if_exists='append', index=False)
        except: print(warining_msg)

    else:
        print('No "flight_type" file need to be added')
        apply_delete_row(remove_table[remove_table['table_name'] == 'dim_slot_time'])
        
# ---------------------------------------------------------------------------------         
        
def load_target_cost():
    # load the raw target
    targets_tmp = pd.read_excel(r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\target_cost\Targets.xlsx',
                                 index_col=None, header=0)

    # transform routes into sector
    targets_1 = targets_tmp.copy()
    targets_1.rename(columns={'Route':'Sector'}, inplace = True)
    targets_1['Sector'] = targets_1['Sector'].str[:3] + targets_1['Sector'].str[4:7]
    targets_1['Revenue'] = targets_1['Revenue'] / 2
    targets_1['Frequency'] = targets_1['Frequency'] / 2
    targets_1['cargo_revenue'] = targets_1['cargo_revenue'] / 2

    targets_2 = targets_1.copy()
    targets_2['Sector'] = targets_1['Sector'].str[3:] + targets_1['Sector'].str[:3]
    targets = pd.concat([targets_1, targets_2])

    # replicate days between valid_date(s)
    date_index = pd.date_range(start=targets['Valid_date'].min(), end=targets['Valid_date'].max() + timedelta(days=31))
    sector_index = targets['Sector'].unique()
    new_index = pd.MultiIndex.from_product([date_index, sector_index], names=['Valid_date', 'Sector'])
    targets_new = targets.set_index(['Valid_date', 'Sector'])
    targets_new = targets_new.reindex(new_index, fill_value=0).reset_index()


    # load the 'costs' file
    costs_1 = pd.read_excel(r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\target_cost\Costs.xlsx',
                                 index_col=None, header=0)
    costs_2 = costs_1.copy()
    costs_2['Sector'] = costs_2['Sector'].str[3:] + costs_2['Sector'].str[:3]

    costs = pd.concat([costs_1, costs_2])
    costs['Total_cost_per_flight'] = costs['Variable_cost_per_flight'] + costs['Fixed_cost_per_flight']

    # replicate days between valid_date(s)
    date_index = pd.date_range(start=costs['Valid_date'].min(), end=costs['Valid_date'].max() + timedelta(days=31))
    sector_index = costs['Sector'].unique()
    new_index = pd.MultiIndex.from_product([date_index, sector_index], names=['Valid_date', 'Sector'])
    costs_new = costs.set_index(['Valid_date', 'Sector'])
    costs_new = costs_new.reindex(new_index).reset_index()
    costs_new.sort_values(by=['Sector', 'Valid_date'], inplace=True)
    costs_new.fillna(method='ffill', inplace=True)


    # combine using join & index
    target_cost = targets_new.set_index(['Valid_date', 'Sector']).join(costs_new.set_index(['Valid_date', 'Sector']))
    target_cost.fillna(value=0, inplace=True)
    target_cost.reset_index(inplace=True)
    target_cost.sort_values(by=['Sector', 'Valid_date'], inplace=True)

    # # load to SQL
    target_cost.to_sql('target_cost', conn, if_exists='replace', index=False)

    print('target_cost has been loaded')




cargo = r'D:\NMT\OneDrive\Viettravel Airline\Database\fact\cargo'
flown_aircraft_leg = r'D:\NMT\OneDrive\Viettravel Airline\Database\fact\flown_aircraft_leg'
inflow_cash = r'D:\NMT\OneDrive\Viettravel Airline\Database\fact\inflow_cash'
pax_revenue = r'D:\NMT\OneDrive\Viettravel Airline\Database\fact\pax_revenue'
payment_detail = r'D:\NMT\OneDrive\Viettravel Airline\Database\fact\payment_detail'
reservation = r'D:\NMT\OneDrive\Viettravel Airline\Database\fact\reservation'

dim_agent = r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\dim_agent'
dim_calendar = r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\dim_calendar'
dim_fare_code = r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\dim_fare_code'
dim_routes = r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\dim_routes'
dim_slot_time = r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\dim_slot_time'
flight_type = r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\flight_type'


dir_list = [cargo, flown_aircraft_leg, inflow_cash, pax_revenue, payment_detail, reservation,
           dim_agent, dim_calendar, dim_routes, dim_fare_code, dim_slot_time, flight_type]
frame_list = []

for dir_x in dir_list:
    frame_list.append(file_name_modified(dir_x))

new_log_table = pd.concat(frame_list, axis=0, ignore_index=True)

query = """
SELECT *
    FROM log_table
"""

previous_log_table = pd.read_sql_query(query, conn)
previous_log_table['modified_time'] = pd.to_datetime(previous_log_table['modified_time'])

diff_table = pd.concat([previous_log_table, new_log_table]).drop_duplicates(ignore_index=True, keep=False)
keep_table = pd.concat([previous_log_table, diff_table, diff_table]).drop_duplicates(ignore_index=True, keep=False)
remove_table = pd.concat([previous_log_table, keep_table]).drop_duplicates(ignore_index=True, keep=False)
add_table = pd.concat([new_log_table, keep_table]).drop_duplicates(ignore_index=True, keep=False)


# load new rows
load_payment_detail()
load_pax_revenue()
load_inflow_cash()
load_cargo()
load_flown_aircraft_leg()
load_reservation()
load_dim_agent()
load_dim_calendar()
load_dim_fare_code()
load_dim_routes()
load_dim_slot_time()
load_flight_type()
load_target_cost()

# write new log_table
new_log_table.to_sql('log_table', conn, if_exists='replace', index=False)
print('write new log_table: done')


# ____________________________________pricing for normal days
# collect all file in folder 'pricing'
pricing_folder = file_name_modified(r'D:\NMT\OneDrive\Viettravel Airline\Database\fact\pricing')['dir_file']
li = []
for folder in pricing_folder:
    li.append(file_name_modified(folder))
price_date_route = pd.concat(li, axis=0, ignore_index=True)

price_date_route.rename(columns={'file_name':'sector', 'table_name':'pricing_date'}, inplace=True)
price_date_route['pricing_date'] = pd.to_datetime(price_date_route['pricing_date'], format='%Y%m%d.%H%M')

# create a function to apply onto DataFrame price_date_route
#     collect all file in subfolders
li = []
def collect_file_dirs(row):
    files_to_read = file_name_modified(row['dir_file'])
    files_to_read['pricing_date'] = row['pricing_date']
    li.append(files_to_read)

price_date_route.apply(collect_file_dirs, axis=1)
pricing = pd.DataFrame()
pricing = pd.concat(li, ignore_index=True)

# create a function to apply onto dataframe pricing
#     read all files in subfolders
li = []
temp_df = pd.DataFrame()
def read_pricing_files(row):
    temp_df = pd.read_csv(row['dir_file'])
    temp_df['sector'] = row['table_name']
    temp_df['pricing_date'] = row['pricing_date']
    temp_df['file_name'] = row['file_name']
    temp_df['modified_time'] = row['modified_time']
    temp_df.rename(columns={'date':'departure_date'}, inplace=True)
    li.append(temp_df)

pricing.apply(read_pricing_files, axis=1)
market_price = pd.concat(li, ignore_index=True)

market_price['price'] = market_price['price'].str.replace(',', '')
market_price = market_price.astype({'price':'int64'})
market_price['departure_datetime'] = pd.to_datetime(market_price['departure_date'] + ' ' + market_price['time'].str[:5])
market_price['VU'] = (market_price['name'].str[:2] == 'VU')*1
market_price['departure_date'] = pd.to_datetime(market_price['departure_date'])

total_market_price = market_price.copy()
total_market_price['adjusted_price'] = total_market_price['price'] - total_market_price['bag'] * 160000
column_order = ['name','bag', 'meal', 'adjusted_price', 'sector', 'departure_datetime', 'pricing_date', 'file_name', 'modified_time']
total_market_price = total_market_price[column_order].reset_index(drop=True)
total_market_price['type'] = 'normal'

column_order = ['name', 'VU','bag', 'meal', 'price', 'sector', 'departure_date', 'departure_datetime', 'pricing_date', 'file_name', 'modified_time']
market_price = market_price[column_order].reset_index(drop=True)

#     load the diff time of VU flights
dim_pricing = pd.read_excel(r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\dim_pricing\dim_pricing.xlsx')

pricing_reference = market_price.set_index('name').join(dim_pricing.set_index('flight_num')).reset_index()
pricing_reference.rename(columns={'index':'flight_num'}, inplace=True)
pricing_reference.dropna(inplace=True)

pricing_reference = pricing_reference[['flight_num', 'sector', 'departure_datetime', 'diff_hours']]
pricing_reference.rename(columns={'flight_num':'VU_compare'}, inplace=True)
pricing_reference['max_time'] = pricing_reference['departure_datetime'] + pd.to_timedelta(pricing_reference['diff_hours'], unit='hours')
pricing_reference['min_time'] = pricing_reference['departure_datetime'] - pd.to_timedelta(pricing_reference['diff_hours'], unit='hours')
pricing_reference['departure_date'] = pd.to_datetime(pricing_reference['departure_datetime'].dt.date)

a = pricing_reference.set_index(keys=['departure_date', 'sector'])[['VU_compare', 'max_time', 'min_time']]
b = market_price.set_index(keys=['departure_date', 'sector'])
c = a.join(b)

market_pricing = c[(c['departure_datetime'] >= c['min_time']) & (c['departure_datetime'] <= c['max_time'])]
market_pricing.reset_index(inplace=True)
# market_pricing['flight_num'] = market_pricing['name'].str[2:]
# market_pricing['airlines'] = market_pricing['name'].str[:2]
market_pricing['adjusted_price'] = market_pricing['price'] - market_pricing['bag'] * 160000
market_pricing['VU_compare_unique_flight_code'] = market_pricing['VU_compare'] + '_' + market_pricing['departure_date'].dt.strftime('%Y%m%d')
market_pricing = market_pricing[['VU_compare_unique_flight_code', 'name', 'bag', 'meal',
                                 'adjusted_price', 'departure_datetime', 'pricing_date', 'file_name', 'modified_time']]
market_pricing.drop_duplicates(inplace=True)
market_pricing['type'] = 'normal'

#     write to SQL
market_pricing.to_sql('market_pricing', conn, if_exists='replace', index=False)
total_market_price.to_sql('total_market_price', conn, if_exists='replace', index=False)
print(f'replicate pricing normal days {total_market_price.shape} : done')






# ____________________________________pricing for Lunar Newyear
# collect all file in folder 'pricing'
pricing_folder = file_name_modified(r'D:\NMT\OneDrive\Viettravel Airline\Database\fact\pricing_special_days\Lunar_newyear')['dir_file']
li = []
for folder in pricing_folder:
    li.append(file_name_modified(folder))
price_date_route = pd.concat(li, axis=0, ignore_index=True)

price_date_route.rename(columns={'file_name':'sector', 'table_name':'pricing_date'}, inplace=True)
price_date_route['pricing_date'] = pd.to_datetime(price_date_route['pricing_date'], format='%Y%m%d.%H%M')

# create a function to apply onto DataFrame price_date_route
#     collect all file in subfolders
li = []
def collect_file_dirs(row):
    files_to_read = file_name_modified(row['dir_file'])
    files_to_read['pricing_date'] = row['pricing_date']
    li.append(files_to_read)

price_date_route.apply(collect_file_dirs, axis=1)
pricing = pd.DataFrame()
pricing = pd.concat(li, ignore_index=True)

# create a function to apply onto dataframe pricing
#     read all files in subfolders
li = []
temp_df = pd.DataFrame()
def read_pricing_files(row):
    temp_df = pd.read_csv(row['dir_file'])
    temp_df['sector'] = row['table_name']
    temp_df['pricing_date'] = row['pricing_date']
    temp_df['file_name'] = row['file_name']
    temp_df['modified_time'] = row['modified_time']
    temp_df.rename(columns={'date':'departure_date'}, inplace=True)
    li.append(temp_df)

pricing.apply(read_pricing_files, axis=1)
market_price = pd.concat(li, ignore_index=True)

market_price['price'] = market_price['price'].str.replace(',', '')
market_price = market_price.astype({'price':'int64'})
market_price['departure_datetime'] = pd.to_datetime(market_price['departure_date'] + ' ' + market_price['time'].str[:5])
market_price['VU'] = (market_price['name'].str[:2] == 'VU')*1
market_price['departure_date'] = pd.to_datetime(market_price['departure_date'])

total_market_price = market_price.copy()
total_market_price['adjusted_price'] = total_market_price['price'] - total_market_price['bag'] * 160000
column_order = ['name','bag', 'meal', 'adjusted_price', 'sector', 'departure_datetime', 'pricing_date', 'file_name', 'modified_time']
total_market_price = total_market_price[column_order].reset_index(drop=True)
total_market_price['type'] = 'Lunar Newyear'

column_order = ['name', 'VU','bag', 'meal', 'price', 'sector', 'departure_date', 'departure_datetime', 'pricing_date', 'file_name', 'modified_time']
market_price = market_price[column_order].reset_index(drop=True)

#     load the diff time of VU flights
dim_pricing = pd.read_excel(r'D:\NMT\OneDrive\Viettravel Airline\Database\dim\dim_pricing\dim_pricing.xlsx')

pricing_reference = market_price.set_index('name').join(dim_pricing.set_index('flight_num')).reset_index()
pricing_reference.rename(columns={'index':'flight_num'}, inplace=True)
pricing_reference.dropna(inplace=True)

pricing_reference = pricing_reference[['flight_num', 'sector', 'departure_datetime', 'diff_hours']]
pricing_reference.rename(columns={'flight_num':'VU_compare'}, inplace=True)
pricing_reference['max_time'] = pricing_reference['departure_datetime'] + pd.to_timedelta(pricing_reference['diff_hours'], unit='hours')
pricing_reference['min_time'] = pricing_reference['departure_datetime'] - pd.to_timedelta(pricing_reference['diff_hours'], unit='hours')
pricing_reference['departure_date'] = pd.to_datetime(pricing_reference['departure_datetime'].dt.date)

a = pricing_reference.set_index(keys=['departure_date', 'sector'])[['VU_compare', 'max_time', 'min_time']]
b = market_price.set_index(keys=['departure_date', 'sector'])
c = a.join(b)

market_pricing = c[(c['departure_datetime'] >= c['min_time']) & (c['departure_datetime'] <= c['max_time'])]
market_pricing.reset_index(inplace=True)
# market_pricing['flight_num'] = market_pricing['name'].str[2:]
# market_pricing['airlines'] = market_pricing['name'].str[:2]
market_pricing['adjusted_price'] = market_pricing['price'] - market_pricing['bag'] * 160000
market_pricing['VU_compare_unique_flight_code'] = market_pricing['VU_compare'] + '_' + market_pricing['departure_date'].dt.strftime('%Y%m%d')
market_pricing = market_pricing[['VU_compare_unique_flight_code', 'name', 'bag', 'meal',
                                 'adjusted_price', 'departure_datetime', 'pricing_date', 'file_name', 'modified_time']]
market_pricing.drop_duplicates(inplace=True)
market_pricing['type'] = 'normal'

#     write to SQL
market_pricing.to_sql('market_pricing', conn, if_exists='append', index=False)
total_market_price.to_sql('total_market_price', conn, if_exists='append', index=False)
print(f'replicate pricing Lunar Newyear {total_market_price.shape} : done')



print('ETL---------------------------------> Done')