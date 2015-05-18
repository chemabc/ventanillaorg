import numpy as np
import datetime
import copy
import time
import json
#from pandas.io.json import json_normalize
from pandas import *
from scipy import stats
from pandas.core.groupby import GroupByError
from collections import defaultdict


pathFolderData = "../data/"#"/media/winD/Dropbox/ETC/ETC_PROYECTOS_propuestas/MedialabPrado/2015_Visualizar15/_VentanillaOrg/"
pathPathData = pathFolderData + "datosTest.json"

# [] for lists
# {} for dictionaries


'''
jotason= [{"tiempo": 3.0187980122799, "tramnum": 6, "resultado": "fail", "proceso": "dememp", "cp": 47073, "rejreason": "fuera de plazo"}, {"tiempo": 2.022929181987997, "tramnum": 5, "resultado": "fail", "proceso": "busform", "cp": 28288, "rejreason": "fuera de plazo"}, {"tiempo": 4.514285141987245, "tramnum": 2, "resultado": "fail", "proceso": "busemp", "cp": 28168, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 4.784342546938054, "tramnum": 2, "resultado": "ok", "proceso": "dememp", "cp": 47009}, {"tiempo": 5.8452084548985415, "tramnum": 3, "resultado": "ok", "proceso": "presta", "cp": 28053}, {"tiempo": 4.485074538229384, "tramnum": 1, "resultado": "ok", "proceso": "busemp", "cp": 28058}, {"tiempo": 4.7949729569402395, "tramnum": 3, "resultado": "fail", "proceso": "busform", "cp": 28111, "rejreason": "fuera de plazo"}, {"tiempo": 6.023655276462101, "tramnum": 6, "resultado": "fail", "proceso": "busemp", "cp": 47087, "rejreason": "fuera de plazo"}, {"tiempo": 3.9581175584358386, "tramnum": 3, "resultado": "ok", "proceso": "busform", "cp": 47034}, {"tiempo": 8.88618771788593, "tramnum": 6, "resultado": "ok", "proceso": "presta", "cp": 28137}, {"tiempo": 5.506551655146254, "tramnum": 3, "resultado": "fail", "proceso": "busemp", "cp": 28119, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 5.731346416603049, "tramnum": 5, "resultado": "ok", "proceso": "presta", "cp": 47023}, {"tiempo": 9.152151297372837, "tramnum": 2, "resultado": "fail", "proceso": "busemp", "cp": 47014, "rejreason": "requisitos incumplido"}, {"tiempo": 2.208636647598368, "tramnum": 4, "resultado": "ok", "proceso": "presta", "cp": 28157}, {"tiempo": 7.146673438959988, "tramnum": 5, "resultado": "ok", "proceso": "dememp", "cp": 28236}, {"tiempo": 4.657654624325294, "tramnum": 2, "resultado": "fail", "proceso": "busform", "cp": 47036, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 5.939025282824389, "tramnum": 5, "resultado": "fail", "proceso": "presta", "cp": 28130, "rejreason": "requisitos incumplido"}, {"tiempo": 7.06666578399229, "tramnum": 1, "resultado": "fail", "proceso": "busform", "cp": 28252, "rejreason": "fuera de plazo"}, {"tiempo": 7.536093741612602, "tramnum": 3, "resultado": "fail", "proceso": "busform", "cp": 28256, "rejreason": "fuera de plazo"}, {"tiempo": 4.366494379278009, "tramnum": 4, "resultado": "fail", "proceso": "busemp", "cp": 28026, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 4.390528512782877, "tramnum": 6, "resultado": "ok", "proceso": "dememp", "cp": 28121}, {"tiempo": 2.1257709886012752, "tramnum": 1, "resultado": "ok", "proceso": "busform", "cp": 47028}, {"tiempo": 9.13534471007055, "tramnum": 4, "resultado": "fail", "proceso": "busemp", "cp": 28168, "rejreason": "requisitos incumplido"}, {"tiempo": 1.7205997749843607, "tramnum": 5, "resultado": "ok", "proceso": "dememp", "cp": 47035}, {"tiempo": 4.6713378155701495, "tramnum": 2, "resultado": "fail", "proceso": "dememp", "cp": 47053, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 2.5403421414500116, "tramnum": 4, "resultado": "ok", "proceso": "busform", "cp": 28128}, {"tiempo": 6.335512245476665, "tramnum": 1, "resultado": "ok", "proceso": "busform", "cp": 47043}, {"tiempo": 4.9227118017976235, "tramnum": 2, "resultado": "ok", "proceso": "busform", "cp": 28076}, {"tiempo": 1.1359575416266994, "tramnum": 5, "resultado": "ok", "proceso": "dememp", "cp": 47085}, {"tiempo": 11.242979971306525, "tramnum": 4, "resultado": "ok", "proceso": "busform", "cp": 28129}, {"tiempo": 4.503616709743942, "tramnum": 4, "resultado": "fail", "proceso": "dememp", "cp": 28029, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 7.165071568095042, "tramnum": 3, "resultado": "fail", "proceso": "presta", "cp": 28110, "rejreason": "fuera de plazo"}, {"tiempo": 2.6982451461438015, "tramnum": 5, "resultado": "ok", "proceso": "presta", "cp": 28317}, {"tiempo": 1.9269029602987517, "tramnum": 1, "resultado": "ok", "proceso": "dememp", "cp": 47095}, {"tiempo": 9.407521272652605, "tramnum": 4, "resultado": "fail", "proceso": "busemp", "cp": 28185, "rejreason": "requisitos incumplido"}, {"tiempo": 2.2165251672424873, "tramnum": 3, "resultado": "ok", "proceso": "busform", "cp": 28261}, {"tiempo": 2.4204141403877917, "tramnum": 6, "resultado": "ok", "proceso": "busemp", "cp": 28125}, {"tiempo": -0.25430833081119886, "tramnum": 1, "resultado": "ok", "proceso": "busemp", "cp": 28102}, {"tiempo": 3.772453291214525, "tramnum": 5, "resultado": "fail", "proceso": "busform", "cp": 47083, "rejreason": "fuera de plazo"}, {"tiempo": 0.670294094925822, "tramnum": 2, "resultado": "fail", "proceso": "busform", "cp": 28119, "rejreason": "fuera de plazo"}, {"tiempo": 6.08852882309329, "tramnum": 4, "resultado": "fail", "proceso": "busform", "cp": 28121, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 5.345866684411538, "tramnum": 1, "resultado": "ok", "proceso": "busemp", "cp": 28029}, {"tiempo": 2.0456559859349985, "tramnum": 1, "resultado": "fail", "proceso": "busform", "cp": 28194, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 6.873679829392465, "tramnum": 1, "resultado": "fail", "proceso": "presta", "cp": 28136, "rejreason": "requisitos incumplido"}, {"tiempo": 7.440349101789571, "tramnum": 5, "resultado": "fail", "proceso": "presta", "cp": 28045, "rejreason": "requisitos incumplido"}, {"tiempo": 4.201353675525535, "tramnum": 1, "resultado": "ok", "proceso": "presta", "cp": 28287}, {"tiempo": 5.918453264701937, "tramnum": 1, "resultado": "ok", "proceso": "presta", "cp": 28024}, {"tiempo": 6.602039578900833, "tramnum": 4, "resultado": "ok", "proceso": "presta", "cp": 28136}, {"tiempo": 3.125387651511377, "tramnum": 2, "resultado": "fail", "proceso": "presta", "cp": 28136, "rejreason": "requisitos incumplido"}, {"tiempo": 1.8327587280089572, "tramnum": 2, "resultado": "fail", "proceso": "presta", "cp": 47095, "rejreason": "fuera de plazo"}, {"tiempo": 1.4643025043459064, "tramnum": 1, "resultado": "ok", "proceso": "busemp", "cp": 28122}, {"tiempo": 0.38277130946032933, "tramnum": 3, "resultado": "ok", "proceso": "busform", "cp": 47071}, {"tiempo": 5.204364380673393, "tramnum": 4, "resultado": "fail", "proceso": "busemp", "cp": 28009, "rejreason": "requisitos incumplido"}, {"tiempo": 1.6939325695829077, "tramnum": 4, "resultado": "fail", "proceso": "dememp", "cp": 28265, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 1.1583858428591038, "tramnum": 5, "resultado": "ok", "proceso": "dememp", "cp": 47060}, {"tiempo": 4.6004214435400455, "tramnum": 2, "resultado": "ok", "proceso": "busemp", "cp": 28225}, {"tiempo": 2.2203772926364564, "tramnum": 1, "resultado": "fail", "proceso": "dememp", "cp": 28253, "rejreason": "requisitos incumplido"}, {"tiempo": 3.3525813204937815, "tramnum": 2, "resultado": "fail", "proceso": "dememp", "cp": 28316, "rejreason": "requisitos incumplido"}, {"tiempo": 6.3110180108094625, "tramnum": 3, "resultado": "ok", "proceso": "busform", "cp": 47039}, {"tiempo": 4.188197349329777, "tramnum": 6, "resultado": "fail", "proceso": "busemp", "cp": 28277, "rejreason": "requisitos incumplido"}, {"tiempo": 4.599430503191793, "tramnum": 5, "resultado": "ok", "proceso": "dememp", "cp": 28081}, {"tiempo": 4.543946824126101, "tramnum": 1, "resultado": "ok", "proceso": "busform", "cp": 28200}, {"tiempo": 4.655413394459225, "tramnum": 1, "resultado": "ok", "proceso": "presta", "cp": 28208}, {"tiempo": 5.131710171644515, "tramnum": 1, "resultado": "fail", "proceso": "busemp", "cp": 28177, "rejreason": "fuera de plazo"}, {"tiempo": 2.940881754155833, "tramnum": 4, "resultado": "ok", "proceso": "busform", "cp": 28055}, {"tiempo": 5.452038527231006, "tramnum": 3, "resultado": "fail", "proceso": "busform", "cp": 28165, "rejreason": "fuera de plazo"}, {"tiempo": 6.773116858116359, "tramnum": 6, "resultado": "ok", "proceso": "presta", "cp": 28115}, {"tiempo": 5.135365386738321, "tramnum": 2, "resultado": "fail", "proceso": "busemp", "cp": 28209, "rejreason": "requisitos incumplido"}, {"tiempo": 4.95652206178927, "tramnum": 2, "resultado": "fail", "proceso": "busform", "cp": 28140, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 5.954305442881778, "tramnum": 6, "resultado": "ok", "proceso": "dememp", "cp": 47017}, {"tiempo": 1.9999751673714385, "tramnum": 4, "resultado": "fail", "proceso": "dememp", "cp": 47007, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 4.234299260087061, "tramnum": 1, "resultado": "fail", "proceso": "dememp", "cp": 28063, "rejreason": "requisitos incumplido"}, {"tiempo": 10.573802509721801, "tramnum": 4, "resultado": "fail", "proceso": "busform", "cp": 28300, "rejreason": "requisitos incumplido"}, {"tiempo": 3.559917207452518, "tramnum": 1, "resultado": "fail", "proceso": "busform", "cp": 28306, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 5.683003020713942, "tramnum": 3, "resultado": "ok", "proceso": "presta", "cp": 28101}, {"tiempo": 7.5549732156500315, "tramnum": 5, "resultado": "fail", "proceso": "busemp", "cp": 28256, "rejreason": "fuera de plazo"}, {"tiempo": 5.076124143083289, "tramnum": 6, "resultado": "ok", "proceso": "busform", "cp": 28011}, {"tiempo": 3.1971119490017426, "tramnum": 2, "resultado": "fail", "proceso": "dememp", "cp": 28208, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 5.708955292596557, "tramnum": 3, "resultado": "fail", "proceso": "busform", "cp": 28292, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": -0.8073698392399162, "tramnum": 1, "resultado": "fail", "proceso": "dememp", "cp": 28181, "rejreason": "requisitos incumplido"}, {"tiempo": 7.171559025485644, "tramnum": 2, "resultado": "ok", "proceso": "presta", "cp": 28272}, {"tiempo": 2.8257329046308888, "tramnum": 3, "resultado": "ok", "proceso": "presta", "cp": 28131}, {"tiempo": 4.263620437048564, "tramnum": 6, "resultado": "fail", "proceso": "presta", "cp": 47047, "rejreason": "requisitos incumplido"}, {"tiempo": 8.944985789768928, "tramnum": 2, "resultado": "ok", "proceso": "busform", "cp": 28276}, {"tiempo": 4.432659296734742, "tramnum": 3, "resultado": "fail", "proceso": "dememp", "cp": 47051, "rejreason": "requisitos incumplido"}, {"tiempo": 3.6269688607282173, "tramnum": 4, "resultado": "fail", "proceso": "busemp", "cp": 28302, "rejreason": "requisitos incumplido"}, {"tiempo": 5.98265106736521, "tramnum": 6, "resultado": "ok", "proceso": "busemp", "cp": 47060}, {"tiempo": 4.88067473632053, "tramnum": 5, "resultado": "ok", "proceso": "busform", "cp": 28217}, {"tiempo": 3.2502493031230455, "tramnum": 5, "resultado": "ok", "proceso": "presta", "cp": 28266}, {"tiempo": 3.267035346609134, "tramnum": 4, "resultado": "ok", "proceso": "dememp", "cp": 28146}, {"tiempo": 10.205300955089257, "tramnum": 3, "resultado": "ok", "proceso": "busemp", "cp": 28081}, {"tiempo": 3.397989057598859, "tramnum": 3, "resultado": "fail", "proceso": "busform", "cp": 28038, "rejreason": "fuera de plazo"}, {"tiempo": 6.539012205965911, "tramnum": 3, "resultado": "fail", "proceso": "presta", "cp": 28059, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 0.8715844247194839, "tramnum": 6, "resultado": "ok", "proceso": "busemp", "cp": 28301}, {"tiempo": -2.1453665676084546, "tramnum": 1, "resultado": "fail", "proceso": "dememp", "cp": 28123, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 15.72420658137563, "tramnum": 2, "resultado": "fail", "proceso": "busform", "cp": 28184, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 0.5472228209085772, "tramnum": 1, "resultado": "fail", "proceso": "presta", "cp": 28122, "rejreason": "documentacion err\\u00f3nea o faltante"}, {"tiempo": 4.9971310483169145, "tramnum": 4, "resultado": "ok", "proceso": "presta", "cp": 47095}, {"tiempo": 5.153874942746305, "tramnum": 2, "resultado": "fail", "proceso": "dememp", "cp": 47067, "rejreason": "requisitos incumplido"}, {"tiempo": 8.659383060235427, "tramnum": 6, "resultado": "ok", "proceso": "busform", "cp": 28262}]
data_string =json.dumps(jotason) 
print("Encoded: ", data_string)
jsonLoaded = json.loads(data_string)
print("Decoded:" , jsonLoaded)
print(jsonLoaded[1])
print(jsonLoaded[1]['tiempo'])
'''
with open(pathPathData) as json_file:
    json_data = json.load(json_file)
    print(json_data)
    print( json_data[1])
    print( json_data[1]["rejreason"])

p_data = pandas.io.json.json_normalize(json_data) 
print(format(p_data))
'''
p_dataSize = p_data.size
print("P_data SIZE: " , p_dataSize.first())
#print("Mean:", p_data["tiempo"].mean())
#print("Variance:", p_data["tiempo"].var())

'''
#p_data['nuevo'] = np.random.rand(p_data.shape[0])
print(p_data.resultado)
print("Shape: " , p_data.shape)
print(p_data.index) #ROWS
print(p_data.columns) #COlumns


#CALCULO DE PORCENTAJES DE RECHAZADO Y ACEPTADO
groupByResultado = p_data.groupby(['resultado'])
print("Agrupados por RESULTADO: ", groupByResultado.size())
#print("Grupos con OK (rows): ", groupByResultado.groups['ok'] )
#print("Grupos con Fail (rows): ", groupByResultado.groups['fail'])

for nombre, datos in groupByResultado:
    print("ITERANDO:")
    print("Nombre: ", nombre)
    print("Datos: ", datos)
print("Grupo FAIL: ", groupByResultado.get_group('fail'))
print("Tiempo en grupo FAIL: ", groupByResultado.get_group('fail').tiempo)
numTotal = len(p_data.resultado)
numFails = len(groupByResultado.get_group('fail').tiempo)
numOks = len(groupByResultado.get_group('ok').tiempo)
print("Numero de fails:" , numFails)
print("Numero de fok:" , len(groupByResultado.get_group('ok').tiempo))
print("Porcentaje FAIL: ", numFails / numTotal * 100 , "%")
print("Porcentaje OK: ", numOks / numTotal * 100 , "%")

#AGRUPAR POR CÓDIGO POSTAL Y MEDIAS DE TIEMPO
groupByCpAndResult= p_data.groupby(['cp', 'resultado'])
print(groupByCpAndResult['resultado'].describe())
dataCpAndResults = groupByCpAndResult['tiempo'].describe()
dataCpAndResultsFile = '../data/dataCpAndResult.json'
meanGroupByCpAndResult = groupByCpAndResult['tiempo'].mean()
print(meanGroupByCpAndResult)
print(type(groupByCpAndResult))
print(type(meanGroupByCpAndResult))

print(meanGroupByCpAndResult[1])
jsonDataFormated = 'data:['
#for index, value in meanGroupByCpAndResult.iteritems():
    #print(index[0])
 #   print(value)
    
    
#AGRUPANDO DIFERENTE
'''
groupByCp = p_data.groupby('cp')
for index, value in groupByCp:
    print(index)
    print(value)
groupByCpResult = groupby(groupByCp, 'results')
''' 
'''

#Funciones para recorrer el sistema multivariable y grabar lo que toque
#http://stackoverflow.com/questions/24374062/pandas-groupby-to-nested-json
jsonFriend = meanGroupByCpAndResult.to_dict()#reset_index.to_json(orient= 'index')
print(jsonFriend)
'''
print(type(meanGroupByCpAndResult))
for a, b in meanGroupByCpAndResult.iteritems():
    #print(b)
    print("a0: ", a[0],"; a1: ",a[1], "; b:", b)

#jsonFriendHormonado = { a[0]: {a[1]:b} for a, b in meanGroupByCpAndResult.iteritems()}
#jsonFriendHormonado = { ['cp', a[0]]: {a[1]:b} for a, b in meanGroupByCpAndResult.iteritems()}
#print(jsonFriendHormonado)

#http://stackoverflow.com/questions/11312733/python-convert-a-list-of-python-dictionaries-to-an-array-of-json-objects
jsn = [ {'cp':a[0], 'resultado':a[1], 'timeMean': b }for a, b in meanGroupByCpAndResult.iteritems()]
print(jsn) 



jsonFriend = json.dumps(jsn)#jsonFriendHormonado.json(orient= 'index')
print(jsonFriend)
try:
    with(open(dataCpAndResultsFile, 'w')) as outfile:
        json.dump(jsn, outfile)
except:
    print("Error al abrir el archivo:", dataCpAndResultsFile)


# CALCULO DE PORCENTAJES ACIERTO Y ERROR y DIVISION POR TRAMITES
print(format(p_data))
groupByResultado= p_data.groupby(['resultado'])
print("GROUPS: ", groupByResultado.groups)
groupsByResultado = dict(list(groupByResultado))
g_ok = groupsByResultado['ok']
g_fail = groupsByResultado['fail']
numTotal = len(p_data)
numFail = len(g_fail)
numOk = len(g_ok)
print(groupsByResultado['fail'])
print("Total: ", numTotal)
print("%Fail: ", numFail/numTotal)
print("%Ok: ", numOk/numTotal)
groupByReject = g_fail.groupby("rejreason")
print("G_FAIL num: ", g_fail.describe())
print("groupByReject: ", groupByReject.size())
print(groupByReject.count())
jsonRej = [{"name": rejreason, "val": (int(value*1000/numFail))/10} for rejreason, value in groupByReject.size().iteritems()]
print(jsonRej)
jsonSummary = [ {"name": resultado, "val": (int(value*1000/numTotal))/10 }for resultado, value  in groupByResultado.size().iteritems()]
print(jsonSummary)


finalList = []

print("G_fail:" , g_fail)
print("BUCLE:")
for row in jsonSummary:
    if(row['name'] == 'fail'):
        for row2 in jsonRej:
            row2['children']=[]
            finalList.append(row2)
        row['children'] = finalList
        print(row)
    else:
        row['children'] = []
            #row[row2]
            #print(row)
            
listRows = []
for row in jsonSummary:
    listRows.append(row)
    
jsonTotal = {'name':'root', 'val':0, 'children':listRows}


print(jsonTotal)
dataResultsFile = '../data/dataResults.json'
jsonSummaryFriend = json.dumps(jsonTotal)#jsonFriendHormonado.json(orient= 'index')
print(jsonSummaryFriend)
try:
    with(open(dataResultsFile, 'w')) as outfile:
        json.dump(jsonTotal, outfile)
except:
    print("Error al abrir el archivo:", dataResultsFile)
