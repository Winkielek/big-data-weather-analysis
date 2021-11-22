# BD_project_weather_forecast

Project members:
- Wojciech Szczypek
- Mikołaj Jakubowski

## Project PoC

Aim of this project is to get weather data and verify weather forecast for the upcoming x days.
Firstly we will create NiFi pipeline for connecting to the API, processing the data and storing it on the hadoop. Next after x days we will trigger similiar pipeline focused on dwonloading historical data. Having data on hadoop it will be uplaoded to HBase using python scripts. and finally it will be analysed with the use of PySpark library. Our analyzis will focus on the differences between forecasts and actual weather. We will try to find the best meteorological station with the smallest forecast error.
