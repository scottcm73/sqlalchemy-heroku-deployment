import datetime as dt
import decimal
import numpy as np
import pandas as pd
import os

import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, func
from sqlalchemy import inspect, desc
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.types import Date, DateTime
from pprint import pprint

from flask import Flask, render_template, url_for, jsonify, request

db_path = os.path.join("Resources", "hawaii.sqlite")
engine = create_engine(f"sqlite:///{db_path}")


def the_start(start_date):
    start = (
        session.query(
            func.min(Measurement.tobs),
            func.avg(Measurement.tobs),
            func.max(Measurement.tobs),
        )
        .filter(Measurement.date >= (dt.datetime.strptime(start_date, "%Y-%m-%d")))
        .order_by(Measurement.date)
        .filter(Measurement.station == "USC00519281")
    )
    return start


def station_activity():
    station_frequency_count = (
        session.query(
            Measurement.id, Measurement.station, func.count(Measurement.station)
        )
        .group_by(Measurement.station)
        .order_by(desc(func.count(Measurement.station)))
        .all()
    )
    return station_frequency_count


def last_year():
    # With previous query I found that the latest date was 2017-08-23
    test_rows = (
        session.query(Measurement.date, Measurement.prcp)
        .filter(Measurement.date >= (dt.datetime.strptime("2016-08-23", "%Y-%m-%d")))
        .order_by(Measurement.date)
    )
    return test_rows


def to_dataframe(this_query):

    # This function only works if the query is not a list object
    df = pd.read_sql(this_query.statement, this_query.session.bind)

    return df


def temp_data():
    # This finds all the temperature data for the last year of observations at one station.
    the_temps = (
        session.query(Measurement.date, Measurement.tobs)
        .filter(Measurement.date >= (dt.datetime.strptime("2016-08-23", "%Y-%m-%d")))
        .order_by(Measurement.date)
        .filter(Measurement.station == "USC00519281")
    )

    return the_temps


Base = declarative_base()


class DictMixIn:
    def to_dict(self):
        return {
            column.name: getattr(self, column.name)
            if not isinstance(getattr(self, column.name), datetime.datetime)
            else getattr(self, column.name).isoformat()
            for column in self.__table__.columns
        }


class Measurement(Base, DictMixIn):
    __tablename__ = "measurement"
    id = Column(Integer, primary_key=True)
    station = Column(String(30))
    date = Column(Date)
    prcp = Column(Float)
    tobs = Column(Float)


class Station(Base, DictMixIn):
    __tablename__ = "station"
    id = Column(Integer, primary_key=True)
    station = Column(String(30))
    name = Column(String(50))
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)




app = Flask(__name__)
@app.before_first_request
def init_app():
    db_path = os.path.join("Resources", "hawaii.sqlite")
    engine = create_engine(f"sqlite:///{db_path}")
    session = Session(engine)

@app.route("/")
def main():
    
    return render_template("index.html")


@app.route("/api/v1.0/precipitation")
def prcp():
    this_test = last_year()
    this_test_df = to_dataframe(this_test)
    this_test_df.set_index("date")
    this_test_df.fillna(0)

    this_test_df["month"] = pd.to_datetime(this_test_df["date"]).dt.to_period("M")
    test = this_test_df.groupby(["month"], as_index=False)["prcp"].agg("sum")
    test.columns = ["month", "precipitation"]

    return render_template(
        "precipitation.html", test_json=test_json, test=test.to_html()
    )


@app.route("/api/v1.0/stations2")
def stations2():

    station_frequency_count = station_activity()
    jsonify(station_frequency_count)

    return jsonify(station_frequency_count)


@app.route("/api/v1.0/stations")
def stations():

    station_frequency_count = station_activity()

    return render_template(
        "stations.html", station_frequency_count=station_frequency_count
    )


@app.route("/api/v1.0/tobs")
def tobs():

    station = "USC00519281"
    the_temps = temp_data()

    return render_template("tobs.html", station=station, the_temps=the_temps)


@app.route("/api/v1.0/tobs2")
def tobs2():
    the_temps2 = temp_data()

    the_temps2_df = to_dataframe(the_temps2)
    temps_dict = the_temps2_df.to_dict(orient="index")

    return temps_dict


@app.route("/api/v1.0/start")
def starts():
    start_date=request.args.get('start_date')
    
    start = the_start(start_date)

    return render_template("starts.html", start=start, start_date=start_date)


@app.route("/api/v1.0/start2")
def starts2():
    start_date=request.args.get('start_date')
    start = the_start(start_date)
    start_df = to_dataframe(start)
    start_dict = start_df.to_dict()
    return start_dict


@app.route("/api/v1.0/start-end")
def start_end():
    start_date=request.args.get('start_date')
    end_date=request.args.get('end_date')
    
    start_end = (
        session.query(
            func.min(Measurement.tobs),
            func.avg(Measurement.tobs),
            func.max(Measurement.tobs),
        )
        .filter(Measurement.date >= (dt.datetime.strptime(start_date, "%Y-%m-%d")))
        .filter(Measurement.date <= (dt.datetime.strptime(end_date, "%Y-%m-%d")))
        .order_by(Measurement.date)
        .filter(Measurement.station == "USC00519281")
    )

    return render_template(
        "end.html", start=start_end, start_date=start_date, end_date=end_date
    )

@app.route("/api/v1.0/start-end2")
def start_end2():
    start_date=request.args.get('start_date')
    end_date=request.args.get('end_date')
    
    start_end = (
        session.query(
            func.min(Measurement.tobs),
            func.avg(Measurement.tobs),
            func.max(Measurement.tobs),
        )
        .filter(Measurement.date >= (dt.datetime.strptime(start_date, "%Y-%m-%d")))
        .filter(Measurement.date <= (dt.datetime.strptime(end_date, "%Y-%m-%d")))
        .order_by(Measurement.date)
        .filter(Measurement.station == "USC00519281")
    )
    start_end_df=to_dataframe(start_end)
    start_end_dict=start_end_df.to_dict()
    return start_end_dict
    


if __name__ == "__main__":
    app.run(debug=True)
