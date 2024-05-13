import streamlit as st
import logging
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


def get_users(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM "User"')
        rows = cur.fetchall()
        conn.commit()
        rows.sort(key=lambda x: x["createdAt"])
        return rows


def get_answers(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM "Answer"')
        rows = cur.fetchall()
        conn.commit()
        rows.sort(key=lambda x: x["createdAt"])
        return rows


# get the total number of users each day and the total number of answers each day


def format_date(date):
    formatted_date = date.strftime("%Y-%m-%d")
    return formatted_date


def get_users_per_day_vs_answers_per_day(users, answers):
    users_per_day = {}
    answers_per_day = {}
    total_users = 0
    for user in users:
        total_users += 1
        user_date = format_date(user["createdAt"])
        if user_date in users_per_day:
            users_per_day[user_date] += 1
        else:
            # get the numbers of total users from the previous day
            users_per_day[user_date] = total_users

    for answer in answers:
        answer_date = format_date(answer["createdAt"])
        if answer_date in answers_per_day:
            answers_per_day[answer_date] += 1
        else:
            answers_per_day[answer_date] = 1
    return users_per_day, answers_per_day


def main():
    try:
        # Attempt to connect to cluster with connection string provided to
        # script. By default, this script uses the value saved to the
        # DATABASE_URL environment variable.
        # For information on supported connection string formats, see
        # https://www.cockroachlabs.com/docs/stable/connect-to-the-database.html.

        conn = psycopg2.connect(
            os.environ["DATABASE_URL"],
            application_name="$ docs_simplecrud_psycopg2",
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        users = get_users(conn)
        answers = get_answers(conn)
        users_per_day, answers_per_day = get_users_per_day_vs_answers_per_day(
            users, answers
        )
        # Create DataFrames from dictionaries
        users_df = pd.DataFrame(users_per_day.items(), columns=["Date", "Users"])
        answers_df = pd.DataFrame(answers_per_day.items(), columns=["Date", "Answers"])

        # Merge DataFrames on 'Date' column
        merged_df = pd.merge(users_df, answers_df, on="Date", how="outer")

        # Fill NaN values with 0
        merged_df.fillna(0, inplace=True)

        # Set 'Date' column as index
        merged_df.set_index("Date", inplace=True)

        st.write("""# Catch-Up Users VS Answers per Day""")
        st.line_chart(merged_df)
    except Exception as e:
        logging.fatal("database connection failed")
        logging.fatal(e)
        return


if __name__ == "__main__":
    main()
