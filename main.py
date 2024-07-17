import streamlit as st
import pandas as pd
import pymysql

def connect_to_database():
    try:
        # Configuración de la conexión a la base de datos
        connection = pymysql.connect(
            host='mysql-inteligenciadenegocios.alwaysdata.net',
            port=3306,
            user='367255_admin',
            password='fajikej627',
            database='inteligenciadenegocios_access',
            connect_timeout=6000  # Tiempo de espera en segundos
        )
        print("Connected to database")
        return connection
    except pymysql.MySQLError as e:
        print("Error connecting to database:", e)
        return None

def upload_csv():
    uploaded_file = st.file_uploader("Elige un archivo CSV", type="csv")
    if uploaded_file is not None:
        st.success("Archivo subido con éxito!")
    return uploaded_file

def load_raw_data_to_staging(csv_file, connection, data_type):
    df = pd.read_csv(csv_file, encoding='latin1')
    df = df.where(pd.notnull(df), None)  # Manejar valores nulos
    df = df.fillna('NULL')  # Manejar valores nulos
    cursor = connection.cursor()

    if data_type == "Liga":
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO Staging_Liga (name,season,status,format,number_of_clubs,total_matches,matches_completed,game_week,total_game_week,progress,average_goals_per_match,average_scored_home_team,average_scored_away_team,btts_percentage,clean_sheets_percentage,prediction_risk,home_scored_advantage_percentage,home_defence_advantage_percentage,home_advantage_percentage,average_corners_per_match,average_corners_per_match_home_team,average_corners_per_match_away_team,total_corners_for_season,average_cards_per_match,average_cards_per_match_home_team,average_cards_per_match_away_team,total_cards_for_season,over_05_percentage,over_15_percentage,over_25_percentage,over_35_percentage,over_45_percentage,over_55_percentage,under_05_percentage,under_15_percentage,under_25_percentage,under_35_percentage,under_45_percentage,under_55_percentage,over_65_corners_percentage,over_75_corners_percentage,over_85_corners_percentage,over_95_corners_percentage,over_105_corners_percentage,over_115_corners_percentage,over_125_corners_percentage,over_135_corners_percentage,over_05_cards_percentage,over_15_cards_percentage,over_25_cards_percentage,over_35_cards_percentage,over_45_cards_percentage,over_55_cards_percentage,over_65_cards_percentage,over_75_cards_percentage,goals_min_0_to_10,goals_min_11_to_20,goals_min_21_to_30,goals_min_31_to_40,goals_min_41_to_50,goals_min_51_to_60,goals_min_61_to_70,goals_min_71_to_80,goals_min_81_to_90,goals_min_0_to_15,goals_min_16_to_30,goals_min_31_to_45,goals_min_46_to_60,goals_min_61_to_75,goals_min_76_to_90,xg_avg_per_match)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = tuple(row)
            # Validar la longitud de la tupla
            if len(values) != 71:
                st.error(f"El número de columnas en la fila no coincide con el número de marcadores de posición en la consulta. Columnas esperadas: 71, Recibidas: {len(values)}")
                st.error(f"Row data: {values}")
                continue
            # Imprimir la consulta y la tupla para depuración
            print(f"Query: {insert_query}")
            print(f"Values: {values}")
            try:
                cursor.execute(insert_query, values)
            except Exception as e:
                st.error(f"Error inserting row: {e}")
                st.error(f"Row data: {values}")
    connection.commit()
    cursor.close()
    st.success(f"Datos de {data_type} cargados a la tabla staging.")


def load_raw_data_to_staging_equipos(csv_file, connection):
    df = pd.read_csv(csv_file, encoding='latin1')
    df = df.where(pd.notnull(df), None)  # Manejar valores nulos
    df = df.fillna('NULL')  # Manejar valores nulos
    cursor = connection.cursor()

    insert_query = """
    INSERT INTO Staging_Equipo (team_name,common_name,season,country,matches_played,matches_played_home,matches_played_away,suspended_matches,wins,wins_home,wins_away,draws,draws_home,draws_away,losses,losses_home,losses_away,points_per_game,points_per_game_home,points_per_game_away,league_position,league_position_home,league_position_away,performance_rank,goals_scored,goals_conceded,goal_difference,total_goal_count,total_goal_count_home,total_goal_count_away,goals_scored_home,goals_scored_away,goals_conceded_home,goals_conceded_away,goal_difference_home,goal_difference_away,minutes_per_goal_scored,minutes_per_goal_scored_home,minutes_per_goal_scored_away,minutes_per_goal_conceded,minutes_per_goal_conceded_home,minutes_per_goal_conceded_away,clean_sheets,clean_sheets_home,clean_sheets_away,btts_count,btts_count_home,btts_count_away,fts_count,fts_count_home,fts_count_away,first_team_to_score_count,first_team_to_score_count_home,first_team_to_score_count_away,corners_total,corners_total_home,corners_total_away,cards_total,cards_total_home,cards_total_away,average_possession,average_possession_home,average_possession_away,shots,shots_home,shots_away,shots_on_target,shots_on_target_home,shots_on_target_away,shots_off_target,shots_off_target_home,shots_off_target_away,fouls,fouls_home,fouls_away,goals_scored_half_time,goals_scored_half_time_home,goals_scored_half_time_away,goals_conceded_half_time,goals_conceded_half_time_home,goals_conceded_half_time_away,goal_difference_half_time,goal_difference_half_time_home,goal_difference_half_time_away,leading_at_half_time,leading_at_half_time_home,leading_at_half_time_away,draw_at_half_time,draw_at_half_time_home,draw_at_half_time_away,losing_at_half_time,losing_at_half_time_home,losing_at_half_time_away,points_per_game_half_time,points_per_game_half_time_home,points_per_game_half_time_away,average_total_goals_per_match,average_total_goals_per_match_home,average_total_goals_per_match_away,goals_scored_per_match,goals_scored_per_match_home,goals_scored_per_match_away,goals_conceded_per_match,goals_conceded_per_match_home,goals_conceded_per_match_away,total_goals_per_match_half_time,total_goals_per_match_half_time_home,total_goals_per_match_half_time_away,goals_scored_per_match_half_time,goals_scored_per_match_half_time_home,goals_scored_per_match_half_time_away,goals_conceded_per_match_half_time,goals_conceded_per_match_half_time_home,goals_conceded_per_match_half_time_away,over05_count,over15_count,over25_count,over35_count,over45_count,over55_count,over05_count_home,over15_count_home,over25_count_home,over35_count_home,over45_count_home,over55_count_home,over05_count_away,over15_count_away,over25_count_away,over35_count_away,over45_count_away,over55_count_away,under05_count,under15_count,under25_count,under35_count,under45_count,under55_count,under05_count_home,under15_count_home,under25_count_home,under35_count_home,under45_count_home,under55_count_home,under05_count_away,under15_count_away,under25_count_away,under35_count_away,under45_count_away,under55_count_away,over05_percentage,over15_percentage,over25_percentage,over35_percentage,over45_percentage,over55_percentage,over05_percentage_home,over15_percentage_home,over25_percentage_home,over35_percentage_home,over45_percentage_home,over55_percentage_home,over05_percentage_away,over15_percentage_away,over25_percentage_away,over35_percentage_away,over45_percentage_away,over55_percentage_away,under05_percentage,under15_percentage,under25_percentage,under35_percentage,under45_percentage,under55_percentage,under05_percentage_home,under15_percentage_home,under25_percentage_home,under35_percentage_home,under45_percentage_home,under55_percentage_home,under05_percentage_away,under15_percentage_away,under25_percentage_away,under35_percentage_away,under45_percentage_away,under55_percentage_away,over05_count_half_time,over15_count_half_time,over25_count_half_time,over05_count_half_time_home,over15_count_half_time_home,over25_count_half_time_home,over05_count_half_time_away,over15_count_half_time_away,over25_count_half_time_away,over05_half_time_percentage,over15_half_time_percentage,over25_half_time_percentage,over05_half_time_percentage_home,over15_half_time_percentage_home,over25_half_time_percentage_home,over05_half_time_percentage_away,over15_half_time_percentage_away,over25_half_time_percentage_away,win_percentage,win_percentage_home,win_percentage_away,home_advantage_percentage,clean_sheet_percentage,clean_sheet_percentage_home,clean_sheet_percentage_away,btts_percentage,btts_percentage_home,btts_percentage_away,fts_percentage,fts_percentage_home,fts_percentage_away,first_team_to_score_percentage,first_team_to_score_percentage_home,first_team_to_score_percentage_away,clean_sheet_half_time,clean_sheet_half_time_home,clean_sheet_half_time_away,clean_sheet_half_time_percentage,clean_sheet_half_time_percentage_home,clean_sheet_half_time_percentage_away,fts_half_time,fts_half_time_home,fts_half_time_away,fts_half_time_percentage,fts_half_time_percentage_home,fts_half_time_percentage_away,btts_half_time,btts_half_time_home,btts_half_time_away,btts_half_time_percentage,btts_half_time_percentage_home,btts_half_time_percentage_away,leading_at_half_time_percentage,leading_at_half_time_percentage_home,leading_at_half_time_percentage_away,draw_at_half_time_percentage,draw_at_half_time_percentage_home,draw_at_half_time_percentage_away,losing_at_half_time_percentage,losing_at_half_time_percentage_home,losing_at_half_time_percentage_away,corners_per_match,corners_per_match_home,corners_per_match_away,cards_per_match,cards_per_match_home,cards_per_match_away,over65_corners_percentage,over75_corners_percentage,over85_corners_percentage,over95_corners_percentage,over105_corners_percentage,over115_corners_percentage,over125_corners_percentage,over135_corners_percentage,xg_for_avg_overall,xg_for_avg_home,xg_for_avg_away,xg_against_avg_overall,xg_against_avg_home,xg_against_avg_away,prediction_risk,goals_scored_min_0_to_10,goals_scored_min_11_to_20,goals_scored_min_21_to_30,goals_scored_min_31_to_40,goals_scored_min_41_to_50,goals_scored_min_51_to_60,goals_scored_min_61_to_70,goals_scored_min_71_to_80,goals_scored_min_81_to_90,goals_conceded_min_0_to_10,goals_conceded_min_11_to_20,goals_conceded_min_21_to_30,goals_conceded_min_31_to_40,goals_conceded_min_41_to_50,goals_conceded_min_51_to_60,goals_conceded_min_61_to_70,goals_conceded_min_71_to_80,goals_conceded_min_81_to_90,draw_percentage_overall,draw_percentage_home,draw_percentage_away,loss_percentage_ovearll,loss_percentage_home,loss_percentage_away,over145_corners_percentage)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
    """

    for _, row in df.iterrows():
        values = tuple(row.values)  # Asegúrate de que esto coincida con las columnas especificadas en la consulta SQL
        cursor.execute(insert_query, values)

    connection.commit()
    cursor.close()
    st.success("Datos de equipos cargados a la tabla staging exitosamente.")


def load_raw_data_to_staging_jugadores(csv_file, connection):
    df = pd.read_csv(csv_file, encoding='latin1')
    # Renombrar columnas para que coincidan con los nombres en la base de datos
    df.rename(columns={
        'Current Club': 'current_club',
        # Agrega aquí cualquier otra columna que necesite ser renombrada
    }, inplace=True)
    df = df.where(pd.notnull(df), None)  # Manejar valores nulos
    df = df.fillna('NULL')  # Manejar valores nulos

    # Define tu query de inserción aquí, asegurando que los nombres de las columnas en el CSV sean los correctos
    columns = ', '.join([f"`{column}`" for column in df.columns])
    placeholders = ', '.join(['%s' for _ in df.columns])
    insert_query = f"""
    INSERT INTO Staging_Jugadores ({columns})
    VALUES ({placeholders})
    """

    # Inserta los datos en la base de datos
    with connection.cursor() as cursor:
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
        connection.commit()

    st.success("Datos de jugadores cargados a la tabla staging exitosamente.")



def main():
    st.title("Data Upload and Processing App")

    view = st.sidebar.selectbox("Selecciona la vista", ["Carga de Datos Crudos", "Procesamiento de Datos"])

    connection = connect_to_database()

    if view == "Carga de Datos Crudos":
        st.header("Carga de Datos Crudos")
        data_type = st.selectbox("Selecciona el tipo de datos a cargar:", ["Liga", "Equipo", "Jugadores"])
        csv_file = upload_csv()
        if csv_file is not None:
            if st.button("Cargar Datos a Staging"):
                if data_type == "Liga":
                    load_raw_data_to_staging(csv_file, connection, data_type)
                elif data_type == "Equipo":
                    load_raw_data_to_staging_equipos(csv_file, connection)
                elif data_type == "Jugadores":
                    load_raw_data_to_staging_jugadores(csv_file, connection)



    elif view == "Procesamiento de Datos":
        st.header("Procesamiento de Datos")
        data_type = st.selectbox("Selecciona el tipo de datos a procesar:", ["Dimensión Tiempo", "Hechos Partidos"])
        csv_file = upload_csv()
        if csv_file is not None:
            df = pd.read_csv(csv_file, encoding='latin1')
            if st.button("Procesar Datos"):
                if data_type == "Dimensión Tiempo":
                    print("xD")
                elif data_type == "Hechos Partidos":
                    print("xH")

    connection.close()

if __name__ == "__main__":
    main()
