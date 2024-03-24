package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/nixpare/broadcaster"
)

const (
    PATH = "./measurements"
)

type WeatherStation struct {
	id       string
	meanTemp float64
}

func (ws WeatherStation) measurement() float64 {
	m := rand.NormFloat64() * 10 + ws.meanTemp
    return math.Round(m * 10.0) / 10.0
}

const BUFFERED_LINES = 2048 * 64

func main() {
    if len(os.Args) < 2 {
        log.Fatalln("Provide only \"<number of records to create> [ <file index suffix> ]\" as arguments")
    }

    size, err := strconv.Atoi(os.Args[1])
    if err != nil {
        log.Fatalln("Invalid <number of records to create>")
    }

    var iterSuffix string
    if len(os.Args) >= 3 {
        index, err := strconv.Atoi(os.Args[2])
        if err != nil {
            log.Fatalln("Invalid <file index suffix>")
        }
        iterSuffix = fmt.Sprintf("-%d", index)
    }

    stations := weatherStations()

    path := PATH + iterSuffix + ".txt"

    func() {
        start := time.Now()

        f, err := os.Create(path)
        if err != nil {
            log.Fatalln(err)
        }
        defer f.Close()

        workers := runtime.NumCPU()
        chunkSize := size / workers
        
        results := broadcaster.NewReceiver[[]byte](workers)
        var wg sync.WaitGroup
        wg.Add(workers+1)

        go func() {
            wg.Wait()
            results.Close()
        }()

        for n := range workers+1 {
            go func() {
                defer wg.Done()

                var b [128 * BUFFERED_LINES]byte
                buf := b[:]
                var wrote int

                for i := n * chunkSize; i < (n + 1) * chunkSize && i < size; i++ {
                    station := stations[rand.Intn(len(stations))]

                    wrote += copy(buf[wrote:], station.id)
                    buf[wrote] = ';'
                    wrote++
                    wrote += copy(buf[wrote:], fmt.Sprintf("%3.1f", station.measurement()))
                    buf[wrote] = '\n'
                    wrote++

                    if iter := i - n * chunkSize; iter % BUFFERED_LINES == 0 && iter != 0 {
                    results.Send(buf[:wrote]).Wait()
                        buf = b[:]
                        wrote = 0
                    }
                }

                results.Send(buf[:wrote]).Wait()
            }()
        }

        for payload := range results.Ch() {
            f.Write(payload.Data())
            payload.Done()
        }

        fmt.Printf("Created file <%s> with %d measurements in %v\n", path, size, time.Since(start));
    }()

    resPath := PATH + iterSuffix + "-result.txt"
    fmt.Printf("Calculating dummy result in %s\n", resPath)

    dummy(path, resPath)
}

func weatherStations() []WeatherStation {
    return []WeatherStation{
        { id: "Abha", meanTemp: 18.0 },
        { id: "Abidjan", meanTemp: 26.0 },
        { id: "Abéché", meanTemp: 29.4 },
        { id: "Accra", meanTemp: 26.4 },
        { id: "Addis Ababa", meanTemp: 16.0 },
        { id: "Adelaide", meanTemp: 17.3 },
        { id: "Aden", meanTemp: 29.1 },
        { id: "Ahvaz", meanTemp: 25.4 },
        { id: "Albuquerque", meanTemp: 14.0 },
        { id: "Alexandra", meanTemp: 11.0 },
        { id: "Alexandria", meanTemp: 20.0 },
        { id: "Algiers", meanTemp: 18.2 },
        { id: "Alice Springs", meanTemp: 21.0 },
        { id: "Almaty", meanTemp: 10.0 },
        { id: "Amsterdam", meanTemp: 10.2 },
        { id: "Anadyr", meanTemp: -6.9 },
        { id: "Anchorage", meanTemp: 2.8 },
        { id: "Andorra la Vella", meanTemp: 9.8 },
        { id: "Ankara", meanTemp: 12.0 },
        { id: "Antananarivo", meanTemp: 17.9 },
        { id: "Antsiranana", meanTemp: 25.2 },
        { id: "Arkhangelsk", meanTemp: 1.3 },
        { id: "Ashgabat", meanTemp: 17.1 },
        { id: "Asmara", meanTemp: 15.6 },
        { id: "Assab", meanTemp: 30.5 },
        { id: "Astana", meanTemp: 3.5 },
        { id: "Athens", meanTemp: 19.2 },
        { id: "Atlanta", meanTemp: 17.0 },
        { id: "Auckland", meanTemp: 15.2 },
        { id: "Austin", meanTemp: 20.7 },
        { id: "Baghdad", meanTemp: 22.77 },
        { id: "Baguio", meanTemp: 19.5 },
        { id: "Baku", meanTemp: 15.1 },
        { id: "Baltimore", meanTemp: 13.1 },
        { id: "Bamako", meanTemp: 27.8 },
        { id: "Bangkok", meanTemp: 28.6 },
        { id: "Bangui", meanTemp: 26.0 },
        { id: "Banjul", meanTemp: 26.0 },
        { id: "Barcelona", meanTemp: 18.2 },
        { id: "Bata", meanTemp: 25.1 },
        { id: "Batumi", meanTemp: 14.0 },
        { id: "Beijing", meanTemp: 12.9 },
        { id: "Beirut", meanTemp: 20.9 },
        { id: "Belgrade", meanTemp: 12.5 },
        { id: "Belize City", meanTemp: 26.7 },
        { id: "Benghazi", meanTemp: 19.9 },
        { id: "Bergen", meanTemp: 7.7 },
        { id: "Berlin", meanTemp: 10.3 },
        { id: "Bilbao", meanTemp: 14.7 },
        { id: "Birao", meanTemp: 26.5 },
        { id: "Bishkek", meanTemp: 11.3 },
        { id: "Bissau", meanTemp: 27.0 },
        { id: "Blantyre", meanTemp: 22.2 },
        { id: "Bloemfontein", meanTemp: 15.6 },
        { id: "Boise", meanTemp: 11.4 },
        { id: "Bordeaux", meanTemp: 14.2 },
        { id: "Bosaso", meanTemp: 30.0 },
        { id: "Boston", meanTemp: 10.9 },
        { id: "Bouaké", meanTemp: 26.0 },
        { id: "Bratislava", meanTemp: 10.5 },
        { id: "Brazzaville", meanTemp: 25.0 },
        { id: "Bridgetown", meanTemp: 27.0 },
        { id: "Brisbane", meanTemp: 21.4 },
        { id: "Brussels", meanTemp: 10.5 },
        { id: "Bucharest", meanTemp: 10.8 },
        { id: "Budapest", meanTemp: 11.3 },
        { id: "Bujumbura", meanTemp: 23.8 },
        { id: "Bulawayo", meanTemp: 18.9 },
        { id: "Burnie", meanTemp: 13.1 },
        { id: "Busan", meanTemp: 15.0 },
        { id: "Cabo San Lucas", meanTemp: 23.9 },
        { id: "Cairns", meanTemp: 25.0 },
        { id: "Cairo", meanTemp: 21.4 },
        { id: "Calgary", meanTemp: 4.4 },
        { id: "Canberra", meanTemp: 13.1 },
        { id: "Cape Town", meanTemp: 16.2 },
        { id: "Changsha", meanTemp: 17.4 },
        { id: "Charlotte", meanTemp: 16.1 },
        { id: "Chiang Mai", meanTemp: 25.8 },
        { id: "Chicago", meanTemp: 9.8 },
        { id: "Chihuahua", meanTemp: 18.6 },
        { id: "Chișinău", meanTemp: 10.2 },
        { id: "Chittagong", meanTemp: 25.9 },
        { id: "Chongqing", meanTemp: 18.6 },
        { id: "Christchurch", meanTemp: 12.2 },
        { id: "City of San Marino", meanTemp: 11.8 },
        { id: "Colombo", meanTemp: 27.4 },
        { id: "Columbus", meanTemp: 11.7 },
        { id: "Conakry", meanTemp: 26.4 },
        { id: "Copenhagen", meanTemp: 9.1 },
        { id: "Cotonou", meanTemp: 27.2 },
        { id: "Cracow", meanTemp: 9.3 },
        { id: "Da Lat", meanTemp: 17.9 },
        { id: "Da Nang", meanTemp: 25.8 },
        { id: "Dakar", meanTemp: 24.0 },
        { id: "Dallas", meanTemp: 19.0 },
        { id: "Damascus", meanTemp: 17.0 },
        { id: "Dampier", meanTemp: 26.4 },
        { id: "Dar es Salaam", meanTemp: 25.8 },
        { id: "Darwin", meanTemp: 27.6 },
        { id: "Denpasar", meanTemp: 23.7 },
        { id: "Denver", meanTemp: 10.4 },
        { id: "Detroit", meanTemp: 10.0 },
        { id: "Dhaka", meanTemp: 25.9 },
        { id: "Dikson", meanTemp: -11.1 },
        { id: "Dili", meanTemp: 26.6 },
        { id: "Djibouti", meanTemp: 29.9 },
        { id: "Dodoma", meanTemp: 22.7 },
        { id: "Dolisie", meanTemp: 24.0 },
        { id: "Douala", meanTemp: 26.7 },
        { id: "Dubai", meanTemp: 26.9 },
        { id: "Dublin", meanTemp: 9.8 },
        { id: "Dunedin", meanTemp: 11.1 },
        { id: "Durban", meanTemp: 20.6 },
        { id: "Dushanbe", meanTemp: 14.7 },
        { id: "Edinburgh", meanTemp: 9.3 },
        { id: "Edmonton", meanTemp: 4.2 },
        { id: "El Paso", meanTemp: 18.1 },
        { id: "Entebbe", meanTemp: 21.0 },
        { id: "Erbil", meanTemp: 19.5 },
        { id: "Erzurum", meanTemp: 5.1 },
        { id: "Fairbanks", meanTemp: -2.3 },
        { id: "Fianarantsoa", meanTemp: 17.9 },
        { id: "Flores,  Petén", meanTemp: 26.4 },
        { id: "Frankfurt", meanTemp: 10.6 },
        { id: "Fresno", meanTemp: 17.9 },
        { id: "Fukuoka", meanTemp: 17.0 },
        { id: "Gabès", meanTemp: 19.5 },
        { id: "Gaborone", meanTemp: 21.0 },
        { id: "Gagnoa", meanTemp: 26.0 },
        { id: "Gangtok", meanTemp: 15.2 },
        { id: "Garissa", meanTemp: 29.3 },
        { id: "Garoua", meanTemp: 28.3 },
        { id: "George Town", meanTemp: 27.9 },
        { id: "Ghanzi", meanTemp: 21.4 },
        { id: "Gjoa Haven", meanTemp: -14.4 },
        { id: "Guadalajara", meanTemp: 20.9 },
        { id: "Guangzhou", meanTemp: 22.4 },
        { id: "Guatemala City", meanTemp: 20.4 },
        { id: "Halifax", meanTemp: 7.5 },
        { id: "Hamburg", meanTemp: 9.7 },
        { id: "Hamilton", meanTemp: 13.8 },
        { id: "Hanga Roa", meanTemp: 20.5 },
        { id: "Hanoi", meanTemp: 23.6 },
        { id: "Harare", meanTemp: 18.4 },
        { id: "Harbin", meanTemp: 5.0 },
        { id: "Hargeisa", meanTemp: 21.7 },
        { id: "Hat Yai", meanTemp: 27.0 },
        { id: "Havana", meanTemp: 25.2 },
        { id: "Helsinki", meanTemp: 5.9 },
        { id: "Heraklion", meanTemp: 18.9 },
        { id: "Hiroshima", meanTemp: 16.3 },
        { id: "Ho Chi Minh City", meanTemp: 27.4 },
        { id: "Hobart", meanTemp: 12.7 },
        { id: "Hong Kong", meanTemp: 23.3 },
        { id: "Honiara", meanTemp: 26.5 },
        { id: "Honolulu", meanTemp: 25.4 },
        { id: "Houston", meanTemp: 20.8 },
        { id: "Ifrane", meanTemp: 11.4 },
        { id: "Indianapolis", meanTemp: 11.8 },
        { id: "Iqaluit", meanTemp: -9.3 },
        { id: "Irkutsk", meanTemp: 1.0 },
        { id: "Istanbul", meanTemp: 13.9 },
        { id: "İzmir", meanTemp: 17.9 },
        { id: "Jacksonville", meanTemp: 20.3 },
        { id: "Jakarta", meanTemp: 26.7 },
        { id: "Jayapura", meanTemp: 27.0 },
        { id: "Jerusalem", meanTemp: 18.3 },
        { id: "Johannesburg", meanTemp: 15.5 },
        { id: "Jos", meanTemp: 22.8 },
        { id: "Juba", meanTemp: 27.8 },
        { id: "Kabul", meanTemp: 12.1 },
        { id: "Kampala", meanTemp: 20.0 },
        { id: "Kandi", meanTemp: 27.7 },
        { id: "Kankan", meanTemp: 26.5 },
        { id: "Kano", meanTemp: 26.4 },
        { id: "Kansas City", meanTemp: 12.5 },
        { id: "Karachi", meanTemp: 26.0 },
        { id: "Karonga", meanTemp: 24.4 },
        { id: "Kathmandu", meanTemp: 18.3 },
        { id: "Khartoum", meanTemp: 29.9 },
        { id: "Kingston", meanTemp: 27.4 },
        { id: "Kinshasa", meanTemp: 25.3 },
        { id: "Kolkata", meanTemp: 26.7 },
        { id: "Kuala Lumpur", meanTemp: 27.3 },
        { id: "Kumasi", meanTemp: 26.0 },
        { id: "Kunming", meanTemp: 15.7 },
        { id: "Kuopio", meanTemp: 3.4 },
        { id: "Kuwait City", meanTemp: 25.7 },
        { id: "Kyiv", meanTemp: 8.4 },
        { id: "Kyoto", meanTemp: 15.8 },
        { id: "La Ceiba", meanTemp: 26.2 },
        { id: "La Paz", meanTemp: 23.7 },
        { id: "Lagos", meanTemp: 26.8 },
        { id: "Lahore", meanTemp: 24.3 },
        { id: "Lake Havasu City", meanTemp: 23.7 },
        { id: "Lake Tekapo", meanTemp: 8.7 },
        { id: "Las Palmas de Gran Canaria", meanTemp: 21.2 },
        { id: "Las Vegas", meanTemp: 20.3 },
        { id: "Launceston", meanTemp: 13.1 },
        { id: "Lhasa", meanTemp: 7.6 },
        { id: "Libreville", meanTemp: 25.9 },
        { id: "Lisbon", meanTemp: 17.5 },
        { id: "Livingstone", meanTemp: 21.8 },
        { id: "Ljubljana", meanTemp: 10.9 },
        { id: "Lodwar", meanTemp: 29.3 },
        { id: "Lomé", meanTemp: 26.9 },
        { id: "London", meanTemp: 11.3 },
        { id: "Los Angeles", meanTemp: 18.6 },
        { id: "Louisville", meanTemp: 13.9 },
        { id: "Luanda", meanTemp: 25.8 },
        { id: "Lubumbashi", meanTemp: 20.8 },
        { id: "Lusaka", meanTemp: 19.9 },
        { id: "Luxembourg City", meanTemp: 9.3 },
        { id: "Lviv", meanTemp: 7.8 },
        { id: "Lyon", meanTemp: 12.5 },
        { id: "Madrid", meanTemp: 15.0 },
        { id: "Mahajanga", meanTemp: 26.3 },
        { id: "Makassar", meanTemp: 26.7 },
        { id: "Makurdi", meanTemp: 26.0 },
        { id: "Malabo", meanTemp: 26.3 },
        { id: "Malé", meanTemp: 28.0 },
        { id: "Managua", meanTemp: 27.3 },
        { id: "Manama", meanTemp: 26.5 },
        { id: "Mandalay", meanTemp: 28.0 },
        { id: "Mango", meanTemp: 28.1 },
        { id: "Manila", meanTemp: 28.4 },
        { id: "Maputo", meanTemp: 22.8 },
        { id: "Marrakesh", meanTemp: 19.6 },
        { id: "Marseille", meanTemp: 15.8 },
        { id: "Maun", meanTemp: 22.4 },
        { id: "Medan", meanTemp: 26.5 },
        { id: "Mek'ele", meanTemp: 22.7 },
        { id: "Melbourne", meanTemp: 15.1 },
        { id: "Memphis", meanTemp: 17.2 },
        { id: "Mexicali", meanTemp: 23.1 },
        { id: "Mexico City", meanTemp: 17.5 },
        { id: "Miami", meanTemp: 24.9 },
        { id: "Milan", meanTemp: 13.0 },
        { id: "Milwaukee", meanTemp: 8.9 },
        { id: "Minneapolis", meanTemp: 7.8 },
        { id: "Minsk", meanTemp: 6.7 },
        { id: "Mogadishu", meanTemp: 27.1 },
        { id: "Mombasa", meanTemp: 26.3 },
        { id: "Monaco", meanTemp: 16.4 },
        { id: "Moncton", meanTemp: 6.1 },
        { id: "Monterrey", meanTemp: 22.3 },
        { id: "Montreal", meanTemp: 6.8 },
        { id: "Moscow", meanTemp: 5.8 },
        { id: "Mumbai", meanTemp: 27.1 },
        { id: "Murmansk", meanTemp: 0.6 },
        { id: "Muscat", meanTemp: 28.0 },
        { id: "Mzuzu", meanTemp: 17.7 },
        { id: "N'Djamena", meanTemp: 28.3 },
        { id: "Naha", meanTemp: 23.1 },
        { id: "Nairobi", meanTemp: 17.8 },
        { id: "Nakhon Ratchasima", meanTemp: 27.3 },
        { id: "Napier", meanTemp: 14.6 },
        { id: "Napoli", meanTemp: 15.9 },
        { id: "Nashville", meanTemp: 15.4 },
        { id: "Nassau", meanTemp: 24.6 },
        { id: "Ndola", meanTemp: 20.3 },
        { id: "New Delhi", meanTemp: 25.0 },
        { id: "New Orleans", meanTemp: 20.7 },
        { id: "New York City", meanTemp: 12.9 },
        { id: "Ngaoundéré", meanTemp: 22.0 },
        { id: "Niamey", meanTemp: 29.3 },
        { id: "Nicosia", meanTemp: 19.7 },
        { id: "Niigata", meanTemp: 13.9 },
        { id: "Nouadhibou", meanTemp: 21.3 },
        { id: "Nouakchott", meanTemp: 25.7 },
        { id: "Novosibirsk", meanTemp: 1.7 },
        { id: "Nuuk", meanTemp: -1.4 },
        { id: "Odesa", meanTemp: 10.7 },
        { id: "Odienné", meanTemp: 26.0 },
        { id: "Oklahoma City", meanTemp: 15.9 },
        { id: "Omaha", meanTemp: 10.6 },
        { id: "Oranjestad", meanTemp: 28.1 },
        { id: "Oslo", meanTemp: 5.7 },
        { id: "Ottawa", meanTemp: 6.6 },
        { id: "Ouagadougou", meanTemp: 28.3 },
        { id: "Ouahigouya", meanTemp: 28.6 },
        { id: "Ouarzazate", meanTemp: 18.9 },
        { id: "Oulu", meanTemp: 2.7 },
        { id: "Palembang", meanTemp: 27.3 },
        { id: "Palermo", meanTemp: 18.5 },
        { id: "Palm Springs", meanTemp: 24.5 },
        { id: "Palmerston North", meanTemp: 13.2 },
        { id: "Panama City", meanTemp: 28.0 },
        { id: "Parakou", meanTemp: 26.8 },
        { id: "Paris", meanTemp: 12.3 },
        { id: "Perth", meanTemp: 18.7 },
        { id: "Petropavlovsk-Kamchatsky", meanTemp: 1.9 },
        { id: "Philadelphia", meanTemp: 13.2 },
        { id: "Phnom Penh", meanTemp: 28.3 },
        { id: "Phoenix", meanTemp: 23.9 },
        { id: "Pittsburgh", meanTemp: 10.8 },
        { id: "Podgorica", meanTemp: 15.3 },
        { id: "Pointe-Noire", meanTemp: 26.1 },
        { id: "Pontianak", meanTemp: 27.7 },
        { id: "Port Moresby", meanTemp: 26.9 },
        { id: "Port Sudan", meanTemp: 28.4 },
        { id: "Port Vila", meanTemp: 24.3 },
        { id: "Port-Gentil", meanTemp: 26.0 },
        { id: "Portland (OR)", meanTemp: 12.4 },
        { id: "Porto", meanTemp: 15.7 },
        { id: "Prague", meanTemp: 8.4 },
        { id: "Praia", meanTemp: 24.4 },
        { id: "Pretoria", meanTemp: 18.2 },
        { id: "Pyongyang", meanTemp: 10.8 },
        { id: "Rabat", meanTemp: 17.2 },
        { id: "Rangpur", meanTemp: 24.4 },
        { id: "Reggane", meanTemp: 28.3 },
        { id: "Reykjavík", meanTemp: 4.3 },
        { id: "Riga", meanTemp: 6.2 },
        { id: "Riyadh", meanTemp: 26.0 },
        { id: "Rome", meanTemp: 15.2 },
        { id: "Roseau", meanTemp: 26.2 },
        { id: "Rostov-on-Don", meanTemp: 9.9 },
        { id: "Sacramento", meanTemp: 16.3 },
        { id: "Saint Petersburg", meanTemp: 5.8 },
        { id: "Saint-Pierre", meanTemp: 5.7 },
        { id: "Salt Lake City", meanTemp: 11.6 },
        { id: "San Antonio", meanTemp: 20.8 },
        { id: "San Diego", meanTemp: 17.8 },
        { id: "San Francisco", meanTemp: 14.6 },
        { id: "San Jose", meanTemp: 16.4 },
        { id: "San José", meanTemp: 22.6 },
        { id: "San Juan", meanTemp: 27.2 },
        { id: "San Salvador", meanTemp: 23.1 },
        { id: "Sana'a", meanTemp: 20.0 },
        { id: "Santo Domingo", meanTemp: 25.9 },
        { id: "Sapporo", meanTemp: 8.9 },
        { id: "Sarajevo", meanTemp: 10.1 },
        { id: "Saskatoon", meanTemp: 3.3 },
        { id: "Seattle", meanTemp: 11.3 },
        { id: "Ségou", meanTemp: 28.0 },
        { id: "Seoul", meanTemp: 12.5 },
        { id: "Seville", meanTemp: 19.2 },
        { id: "Shanghai", meanTemp: 16.7 },
        { id: "Singapore", meanTemp: 27.0 },
        { id: "Skopje", meanTemp: 12.4 },
        { id: "Sochi", meanTemp: 14.2 },
        { id: "Sofia", meanTemp: 10.6 },
        { id: "Sokoto", meanTemp: 28.0 },
        { id: "Split", meanTemp: 16.1 },
        { id: "St. John's", meanTemp: 5.0 },
        { id: "St. Louis", meanTemp: 13.9 },
        { id: "Stockholm", meanTemp: 6.6 },
        { id: "Surabaya", meanTemp: 27.1 },
        { id: "Suva", meanTemp: 25.6 },
        { id: "Suwałki", meanTemp: 7.2 },
        { id: "Sydney", meanTemp: 17.7 },
        { id: "Tabora", meanTemp: 23.0 },
        { id: "Tabriz", meanTemp: 12.6 },
        { id: "Taipei", meanTemp: 23.0 },
        { id: "Tallinn", meanTemp: 6.4 },
        { id: "Tamale", meanTemp: 27.9 },
        { id: "Tamanrasset", meanTemp: 21.7 },
        { id: "Tampa", meanTemp: 22.9 },
        { id: "Tashkent", meanTemp: 14.8 },
        { id: "Tauranga", meanTemp: 14.8 },
        { id: "Tbilisi", meanTemp: 12.9 },
        { id: "Tegucigalpa", meanTemp: 21.7 },
        { id: "Tehran", meanTemp: 17.0 },
        { id: "Tel Aviv", meanTemp: 20.0 },
        { id: "Thessaloniki", meanTemp: 16.0 },
        { id: "Thiès", meanTemp: 24.0 },
        { id: "Tijuana", meanTemp: 17.8 },
        { id: "Timbuktu", meanTemp: 28.0 },
        { id: "Tirana", meanTemp: 15.2 },
        { id: "Toamasina", meanTemp: 23.4 },
        { id: "Tokyo", meanTemp: 15.4 },
        { id: "Toliara", meanTemp: 24.1 },
        { id: "Toluca", meanTemp: 12.4 },
        { id: "Toronto", meanTemp: 9.4 },
        { id: "Tripoli", meanTemp: 20.0 },
        { id: "Tromsø", meanTemp: 2.9 },
        { id: "Tucson", meanTemp: 20.9 },
        { id: "Tunis", meanTemp: 18.4 },
        { id: "Ulaanbaatar", meanTemp: -0.4 },
        { id: "Upington", meanTemp: 20.4 },
        { id: "Ürümqi", meanTemp: 7.4 },
        { id: "Vaduz", meanTemp: 10.1 },
        { id: "Valencia", meanTemp: 18.3 },
        { id: "Valletta", meanTemp: 18.8 },
        { id: "Vancouver", meanTemp: 10.4 },
        { id: "Veracruz", meanTemp: 25.4 },
        { id: "Vienna", meanTemp: 10.4 },
        { id: "Vientiane", meanTemp: 25.9 },
        { id: "Villahermosa", meanTemp: 27.1 },
        { id: "Vilnius", meanTemp: 6.0 },
        { id: "Virginia Beach", meanTemp: 15.8 },
        { id: "Vladivostok", meanTemp: 4.9 },
        { id: "Warsaw", meanTemp: 8.5 },
        { id: "Washington, D.C.", meanTemp: 14.6 },
        { id: "Wau", meanTemp: 27.8 },
        { id: "Wellington", meanTemp: 12.9 },
        { id: "Whitehorse", meanTemp: -0.1 },
        { id: "Wichita", meanTemp: 13.9 },
        { id: "Willemstad", meanTemp: 28.0 },
        { id: "Winnipeg", meanTemp: 3.0 },
        { id: "Wrocław", meanTemp: 9.6 },
        { id: "Xi'an", meanTemp: 14.1 },
        { id: "Yakutsk", meanTemp: -8.8 },
        { id: "Yangon", meanTemp: 27.5 },
        { id: "Yaoundé", meanTemp: 23.8 },
        { id: "Yellowknife", meanTemp: -4.3 },
        { id: "Yerevan", meanTemp: 12.4 },
        { id: "Yinchuan", meanTemp: 9.0 },
        { id: "Zagreb", meanTemp: 10.7 },
        { id: "Zanzibar City", meanTemp: 26.0 },
        { id: "Zürich", meanTemp: 9.3 },
    }
}
