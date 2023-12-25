package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"golang.org/x/time/rate"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Response struct {
	Results      []MovieIndex `json:"results"`
	Page         uint16       `json:"page"`
	TotalPages   uint16       `json:"total_pages"`
	TotalResults uint16       `json:"total_results"`
}

type MovieIndex struct {
	ID    uint32 `json:"id"`
	Adult bool   `json:"adult"`
}

type Movie struct {
	ID                  uint32              `json:"id"`
	OriginalLanguage    *string             `json:"original_language"`
	OriginalTitle       *string             `json:"original_title"`
	Title               string              `json:"title"`
	PosterPath          *string             `json:"poster_path"`
	Popularity          float32             `json:"popularity"`
	Runtime             uint16              `json:"runtime"`
	Budget              uint32              `json:"budget"`
	ReleaseDateStr      string              `json:"release_date"`
	Actors              []Person            `json:"actors"`
	Directors           []Person            `json:"directors"`
	ReleaseCountries    []ReleaseCountry    `json:"release_dates"`
	Genres              []Genre             `json:"genres"`
	ProductionCountries []ProductionCountry `json:"production_countries"`
}

type MovieDB struct {
	ID               uint32  `json:"id"`
	OriginalLanguage *string `json:"original_language" gorm:"column:originalLanguage"`
	OriginalTitle    *string `json:"original_title" gorm:"column:originaltitle"`
	Title            string  `json:"title"`
	PosterPath       *string `json:"poster_path" gorm:"column:posterPath"`
	Popularity       float32 `json:"popularity"`
	Runtime          uint16  `json:"runtime"`
	Budget           uint32  `json:"budget"`
	ReleaseDateStr   *string `json:"release_date" gorm:"column:primaryReleaseDate"`
}

type Genre struct {
	ID   uint32 `json:"id"`
	Name string `json:"name"`
}

type ReleaseCountry struct {
	ISO31661          string             `json:"iso_639_1"`
	LocalReleaseDates []LocalReleaseDate `json:"local_release_dates"`
}

type LocalReleaseDate struct {
	Note        string    `json:"note"`
	ReleaseDate time.Time `json:"release_date"`
	Type        uint8     `json:"type"`
}

type ProductionCountry struct {
	ISO31661 string `json:"iso_3166_1"`
	Name     string `json:"name"`
}

type Person struct {
	ID   uint32 `json:"id"`
	Name string `json:"name"`
}

type MovieActor struct {
	MovieId uint32 `gorm:"column:movieId"`
	ActorId uint32 `gorm:"column:actorId"`
}

type MovieDirector struct {
	MovieId    uint32 `gorm:"column:movieId"`
	DirectorId uint32 `gorm:"column:directorId"`
}

type MovieGenre struct {
	MovieId uint32 `gorm:"column:movieId"`
	GenreId uint32 `gorm:"column:genreId"`
}

type MovieCountry struct {
	MovieId    uint32 `gorm:"column:movieId"`
	CountryIso string `gorm:"column:countryIso"`
}

type MReleaseCountry struct {
	ID       uint32
	ISO31661 string `gorm:"column:iso31661"`
	MovieId  uint32 `gorm:"column:movieId"`
}

type MLocalRelease struct {
	ID               uint32
	Note             *string
	ReleaseDate      time.Time `gorm:"column:releaseDate"`
	Type             uint8
	ReleaseCountryId uint32 `gorm:"column:releaseCountryId"`
}

var (
	limiter    = rate.NewLimiter(rate.Every(time.Second/40), 1)
	totalPages = 500
)

func fetchIndexData(PageNum int) ([]byte, error) {
	if err := limiter.Wait(context.Background()); err != nil {
		fmt.Printf("Rate limit exceeded for Page %d: %v\n", PageNum, err)
	}

	url := fmt.Sprintf("https://api.themoviedb.org/3/movie/changes?page=%d", PageNum)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+os.Getenv("API_ACCESS_TOKEN"))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status code: %d", res.StatusCode)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func fetchAndProcessIndexData(pageNum int, idsCh chan uint32) {
	body, err := fetchIndexData(pageNum)
	if err != nil {
		fmt.Printf("Error fetching the first index page: %v\n", err)
		return
	}
	var rawInitData Response
	err = json.Unmarshal(body, &rawInitData)
	if err != nil {
		fmt.Printf("Error unmarshalling the first index page: %v\n", err)
		return
	}
	if pageNum == 1 {
		totalPages = int(rawInitData.TotalPages)
	}
	for _, entry := range rawInitData.Results {
		if !entry.Adult {
			idsCh <- entry.ID
		}
	}
}

func fetchDetailsData(id uint32) ([]byte, error) {
	if err := limiter.Wait(context.Background()); err != nil {
		fmt.Printf("Rate limit exceeded for Page %d: %v\n", id, err)
	}

	url := fmt.Sprintf("https://api.themoviedb.org/3/movie/%d?append_to_response=relese_dates%%2Ccredits&language=en-US", id)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+os.Getenv("API_ACCESS_TOKEN"))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status code: %d", res.StatusCode)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func filterEmptyDates(input string) *string {
	if input != "" {
		return &input
	} else {
		return nil
	}
}

func fetchAndProcessDetailsData(id uint32, movieBaseCh chan MovieDB, peopleRefCh chan Person, actorCh chan MovieActor, directorCh chan MovieDirector, genreCh chan MovieGenre, countryCh chan MovieCountry, releaseCountryCh chan MReleaseCountry, localReleaseCh chan MLocalRelease) {
	body, err := fetchDetailsData(id)
	if err != nil {
		fmt.Printf("Error fetching details for ID %d: %v\n", id, err)
		return
	}
	var movie Movie
	err = json.Unmarshal(body, &movie)
	if err != nil {
		fmt.Println("Error parsing JSON data for Movie ID:", id, err)
		return
	}

	movieBaseCh <- MovieDB{
		ID:               movie.ID,
		OriginalLanguage: movie.OriginalLanguage,
		OriginalTitle:    movie.OriginalTitle,
		Title:            movie.Title,
		PosterPath:       movie.PosterPath,
		Popularity:       movie.Popularity,
		Runtime:          movie.Runtime,
		Budget:           movie.Budget,
		ReleaseDateStr:   filterEmptyDates(movie.ReleaseDateStr),
	}

	for _, actor := range movie.Actors {
		peopleRefCh <- actor

		actorCh <- MovieActor{
			MovieId: movie.ID,
			ActorId: actor.ID,
		}
	}

	for _, director := range movie.Directors {
		peopleRefCh <- director

		directorCh <- MovieDirector{
			MovieId:    movie.ID,
			DirectorId: director.ID,
		}
	}

	for _, genre := range movie.Genres {
		genreCh <- MovieGenre{
			MovieId: movie.ID,
			GenreId: genre.ID,
		}
	}

	for _, country := range movie.ProductionCountries {
		countryCh <- MovieCountry{
			MovieId:    movie.ID,
			CountryIso: country.ISO31661,
		}
	}

	for i, releaseCountry := range movie.ReleaseCountries {
		releaseCountryIdString := strconv.Itoa(int(movie.ID)) + strconv.Itoa(i)
		releaseCountryId, _ := strconv.Atoi(releaseCountryIdString)

		for n, localRelease := range releaseCountry.LocalReleaseDates {
			localReleaseIdString := strconv.Itoa(int(movie.ID)) + strconv.Itoa(i)
			localReleaseIdPreInt, _ := strconv.Atoi(localReleaseIdString)
			localReleaseId := localReleaseIdPreInt + n

			var localReleaseNote *string

			if localRelease.Note != "" {
				localReleaseNote = &localRelease.Note
			}

			localReleaseCh <- MLocalRelease{
				ID:               uint32(localReleaseId),
				Note:             localReleaseNote,
				ReleaseDate:      localRelease.ReleaseDate,
				Type:             localRelease.Type,
				ReleaseCountryId: uint32(releaseCountryId),
			}
		}

		releaseCountryCh <- MReleaseCountry{
			ID:       uint32(releaseCountryId),
			MovieId:  movie.ID,
			ISO31661: releaseCountry.ISO31661,
		}
	}
}

func main() {
	fmt.Printf("Started executing at %s \n", time.Now().Format("15:04:05"))
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file:", err)
		return
	}

	username := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	database := os.Getenv("POSTGRES_DATABASE")
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=require TimeZone=Asia/Shanghai",
		host, username, password, database, port)
	db, _ := gorm.Open(postgres.Open(dsn), &gorm.Config{
		PrepareStmt:            true,
		SkipDefaultTransaction: true,
	}, nil)
	if err != nil {
		panic(err)
	}

	const batchSize = 500
	idsCh := make(chan uint32, 20000)
	movieBaseCh := make(chan MovieDB, 20000)
	peopleRefCh := make(chan Person, 200000)
	actorCh := make(chan MovieActor, 100000)
	directorCh := make(chan MovieDirector, 100000)
	genreCh := make(chan MovieGenre, 50000)
	countryCh := make(chan MovieCountry, 100000)
	releaseCountryCh := make(chan MReleaseCountry, 1000000)
	localReleaseCh := make(chan MLocalRelease, 1000000)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fetchAndProcessIndexData(1, idsCh)
	}()
	wg.Wait()

	go func() {
		var wgFetch sync.WaitGroup
		for i := 2; i <= totalPages; i++ {
			wgFetch.Add(1)
			go func(i int) {
				defer wgFetch.Done()
				fetchAndProcessIndexData(i, idsCh)
			}(i)
		}
		wgFetch.Wait()
		close(idsCh)
	}()

	go func() {
		var wgDetails sync.WaitGroup
		for id := range idsCh {
			wgDetails.Add(1)
			go func(id uint32) {
				defer wgDetails.Done()
				fetchAndProcessDetailsData(id, movieBaseCh, peopleRefCh, actorCh, directorCh, genreCh, countryCh, releaseCountryCh, localReleaseCh)
			}(id)
		}
		wgDetails.Wait()
		close(movieBaseCh)
		close(peopleRefCh)
		close(actorCh)
		close(directorCh)
		close(genreCh)
		close(countryCh)
		close(releaseCountryCh)
		close(localReleaseCh)
	}()

	var wgWriteBase sync.WaitGroup
	wgWriteBase.Add(1)
	go func() {
		defer wgWriteBase.Done()
		writeBaseRows(db, movieBaseCh, batchSize)
	}()

	wgWriteBase.Add(1)
	go func() {
		defer wgWriteBase.Done()
		writePeopleRefRows(db, peopleRefCh, batchSize)
	}()
	wgWriteBase.Wait()

	var wgWrite sync.WaitGroup
	wgWrite.Add(1)
	go func() {
		defer wgWrite.Done()
		writeMovieActorRows(db, actorCh, batchSize)
		writeMovieDirectorRows(db, directorCh, batchSize)
	}()
	wgWrite.Wait()

	var wgWriteSecond sync.WaitGroup
	wgWriteSecond.Add(1)
	go func() {
		defer wgWriteSecond.Done()
		writeMovieGenreRows(db, genreCh, batchSize)
		writeMovieCountryRows(db, countryCh, batchSize)
		writeReleaseCountryRows(db, releaseCountryCh, batchSize)
	}()
	wgWriteSecond.Wait()

	var wgWriteChild sync.WaitGroup
	wgWriteChild.Add(1)
	go func() {
		defer wgWriteChild.Done()
		writeLocalReleaseRows(db, localReleaseCh, batchSize)
	}()
	wgWriteChild.Wait()
	wg.Wait()

	fmt.Println("Successfully fetched data and written to the DB")
}

func writeBaseRows(db *gorm.DB, dataChannel chan MovieDB, batchSize int) {
	var batch []MovieDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeBasesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []MovieDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeBasesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeBasesBatch(db *gorm.DB, objects []MovieDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{UpdateAll: true}).Table("Movie").Model(&MovieDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writePeopleRefRows(db *gorm.DB, dataChannel chan Person, batchSize int) {
	var batch []Person
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writePeopleRefsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []Person{}
		}
	}

	if len(batch) > 0 {
		if err := writePeopleRefsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writePeopleRefsBatch(db *gorm.DB, objects []Person) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("CinemaPerson").Model(&Person{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeMovieActorRows(db *gorm.DB, dataChannel chan MovieActor, batchSize int) {
	var batch []MovieActor
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeActorsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []MovieActor{}
		}
	}

	if len(batch) > 0 {
		if err := writeActorsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}

func writeActorsBatch(db *gorm.DB, objects []MovieActor) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("MovieActor").Model(&MovieActor{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeMovieDirectorRows(db *gorm.DB, dataChannel chan MovieDirector, batchSize int) {
	var batch []MovieDirector
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeDirectorsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []MovieDirector{}
		}
	}

	if len(batch) > 0 {
		if err := writeDirectorsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}

func writeDirectorsBatch(db *gorm.DB, objects []MovieDirector) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("MovieDirector").Model(&MovieDirector{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeMovieGenreRows(db *gorm.DB, dataChannel chan MovieGenre, batchSize int) {
	var batch []MovieGenre
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeGenresBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []MovieGenre{}
		}
	}

	if len(batch) > 0 {
		if err := writeGenresBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}

func writeGenresBatch(db *gorm.DB, objects []MovieGenre) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("MovieGenre").Model(&MovieGenre{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeMovieCountryRows(db *gorm.DB, dataChannel chan MovieCountry, batchSize int) {
	var batch []MovieCountry
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeCountriesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []MovieCountry{}
		}
	}

	if len(batch) > 0 {
		if err := writeCountriesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}

func writeCountriesBatch(db *gorm.DB, objects []MovieCountry) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("MovieCountry").Model(&MovieCountry{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeReleaseCountryRows(db *gorm.DB, dataChannel chan MReleaseCountry, batchSize int) {
	var batch []MReleaseCountry
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeReleaseCountriesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []MReleaseCountry{}
		}
	}

	if len(batch) > 0 {
		if err := writeReleaseCountriesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}

func writeReleaseCountriesBatch(db *gorm.DB, objects []MReleaseCountry) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("MReleaseCountry").Model(&MReleaseCountry{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeLocalReleaseRows(db *gorm.DB, dataChannel chan MLocalRelease, batchSize int) {
	var batch []MLocalRelease
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeLocalReleasesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []MLocalRelease{}
		}
	}

	if len(batch) > 0 {
		if err := writeLocalReleasesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}

func writeLocalReleasesBatch(db *gorm.DB, objects []MLocalRelease) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("MLocalRelease").Model(&MLocalRelease{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}
