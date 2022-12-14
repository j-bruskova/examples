
"""
Automatically generated by Colaboratory.
"""

# импорт библиотек
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

# открывает csv
data = pd.read_csv('120_progression.csv')
data.head(10)

"""# curve_fit"""
# формула
def func(x, a, b):
    return (x**a)*b

# разделение данных для обучения
xdata = np.array(data['level'])
ydata = data['metric'].to_numpy()

# решает уравнения для каждой серии
pars_full, cov = curve_fit(f=func, xdata=xdata, ydata=ydata)
# выводит на экран коэффициенты
print("a = ", pars_full[0])
print("b = ", pars_full[1])
# print("c = ", pars_full[2])
# на основе решения генерирует данные заново
ydata_prog_full = func(xdata, a=pars_full[0], b=pars_full[1])

"""# Визуализация"""

#размер изображения
plt.figure(figsize=(15, 7))
#подписи
plt.xlabel("level")
plt.ylabel("metric")
#фактические данные
plt.plot(xdata, data.metric, c = 'black',label='original data')
# #данные прогноза
plt.plot(xdata, ydata_prog_full, c = 'red', label='new data')

plt.legend()
plt.show()

"""# Вывод данных"""

#получить результат в виде таблицы
data_new = pd.DataFrame(zip(xdata, ydata_prog_full),  columns =['level', 'our_prog'])
data_new.head()

# создать новый CSV и залить туда новые данные
# data_new.to_csv('formula.csv')
# f = pd.read_csv('formula.csv')


