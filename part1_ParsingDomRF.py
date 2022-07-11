import requests
import csv


def write_csv(fileName, data):
    flag = True
    with open(fileName, 'a', encoding='utf-8-sig', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(data))
        # writer.writeheader()
        writer.writerow(data)


def reestr():
    url = "https://наш.дом.рф/сервисы/api/erz/main/filter"

    offset = 0
    # index = 0
    while True:
        parameters = (
            # "?offset=4300&limit=100&sortField=devShortNm&sortType=asc"
            ('offset', offset),
            ('limit', 100),
            ('sortField', 'devShortNm'),
            ('sortType', 'asc')
        )

        response = requests.get(url, params=parameters)
        reestr = response.json()

        developers = reestr['data']['developers']
        for developer in developers:
            devId = developer.get("devId")
            devShortNm = developer.get("devShortNm")
            buildObjCnt = developer.get("buildObjCnt")
            comissObjCnt = developer.get("comissObjCnt")
            regRegionDesc = developer.get("regRegionDesc")
            devSite = developer.get("devSite")
            devFactAddr = developer.get("devFactAddr")
            devOrgRegRegionCd = developer.get("devOrgRegRegionCd")

            data = {
                "devId": devId,
                "devShortNm": devShortNm,
                "buildObjCnt": buildObjCnt,
                "comissObjCnt": comissObjCnt,
                "regRegionDesc": regRegionDesc,
                "devSite": devSite,
                "devFactAddr": devFactAddr,
                "devOrgRegRegionCd": devOrgRegRegionCd
            }

            # print(data)
            write_csv('dom.rf/csv/reestr_dom.rf.csv', data)

        if len(developers) == 0:
            break
        # index += 1
        # print(index)
        offset += 100


def newBuildings():
    url = "https://наш.дом.рф/сервисы/api/kn/object"

    offset = 0
    # index = 0
    while True:
        parameters = (
            ('offset', offset),
            ('limit', 100),
            ('sortField', 'devId.devShortCleanNm'),
            ('sortType', 'asc'),
            ('objStatus', 0)
        )

        response = requests.get(url, params=parameters)
        new_buildings = response.json()

        objects = new_buildings['data']['list']
        for object_ in objects:
            objId = object_.get("objId")

            inside_object = object_['developer']
            devId = inside_object.get("devId")

            objAddr = object_.get("objAddr")
            objFloorMin = object_.get("objFloorMin")
            objFloorMax = object_.get("objFloorMax")
            objReady100PercDt = object_.get("objReady100PercDt")
            objSquareLiving = object_.get("objSquareLiving")
            latitude = object_.get("latitude")
            longitude = object_.get("longitude")
            buildType = object_.get("buildType")

            data = {
                "objId": objId,
                "devId": devId,
                "objAddr": objAddr,
                "objFloorMin": objFloorMin,
                "objFloorMax": objFloorMax,
                "objReady100PercDt": objReady100PercDt,
                "objSquareLiving": objSquareLiving,
                "latitude": latitude,
                "longitude": longitude,
                "buildType": buildType
            }

            # print(data)
            write_csv('dom.rf/csv/new_buildings_dom.rf.csv', data)

        if len(new_buildings) == 0:
            break
        # index += 1
        # print(index)
        offset += 100


def main():
    # reestr()
    newBuildings()


if __name__ == '__main__':
    main()
