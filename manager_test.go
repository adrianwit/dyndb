package dyndb_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
	"log"
	"path"
	"testing"
)

type Music struct {
	Artist      string `primaryKey:"true"`
	SongTitle   string `primaryKey:"true"`
	AlbumTitle  string
	ReleaseYear int
	Price       float64
	Genre       string
	Tags        string
}

func TestManager(t *testing.T) {

	//dsc.Logf = dsc.StdoutLogger
	config, err := dsc.NewConfigWithParameters("dyndb", "", "aws", map[string]interface{}{})
	if !assert.Nil(t, err) {
		return
	}
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	if err != nil {
		log.Fatal(err)
		return
	}

	parentDir := toolbox.CallerDirectory(3)

	specification := make(map[string]interface{})
	resource := url.NewResource(path.Join(parentDir, "test/schema/music.json"))
	if !assert.Nil(t, resource.Decode(&specification)) {
		return
	}

	//Test insert
	dialect := dsc.GetDatastoreDialect("dyndb")
	err = dialect.DropTable(manager, "", "music")
	fmt.Printf("%v\n", err)
	err = dialect.CreateTable(manager, "", "music", specification)
	if !assert.Nil(t, err) {
		log.Fatal(err)
	}

	_, err = manager.Execute("DELETE FROM music", nil)
	if !assert.Nil(t, err) {
		return
	}

	for i := 0; i < 3; i++ {
		sqlResult, err := manager.Execute("INSERT INTO music(Artist, SongTitle, ReleaseYear, Price)  VALUES(?, ?, ?, ?)",
			fmt.Sprintf("Artist%d", i),
			fmt.Sprintf("Title%d", i),
			2000+i,
			0.5+toolbox.AsFloat(i),
		)
		if !assert.Nil(t, err) {
			log.Fatal(err)
		}
		affected, _ := sqlResult.RowsAffected()
		assert.EqualValues(t, 1, affected)
	}

	queryCases := []struct {
		Description string
		SQL         string
		Parameters  []interface{}
		Expected    []*Music
	}{
		{
			Description: "Read records ",
			SQL:         "SELECT Artist, SongTitle, ReleaseYear, Price FROM music",
			Expected: []*Music{
				{
					Artist:      "Artist0",
					SongTitle:   "Title0",
					ReleaseYear: 2000,
					Price:       0.5,
				},
				{
					Artist:      "Artist1",
					SongTitle:   "Title1",
					ReleaseYear: 2001,
					Price:       1.5,
				},
				{
					Artist:      "Artist2",
					SongTitle:   "Title2",
					ReleaseYear: 2002,
					Price:       2.5,
				},
			},
		},
		{
			Description: "Read single with placeholder",
			SQL:         "SELECT Artist, SongTitle, ReleaseYear, Price FROM music WHERE Artist = ?",
			Parameters:  []interface{}{"Artist0"},
			Expected: []*Music{
				{
					Artist:      "Artist0",
					SongTitle:   "Title0",
					ReleaseYear: 2000,
					Price:       0.5,
				},
			},
		},
		{
			Description: "Read single with placeholder",
			SQL:         "SELECT Artist, SongTitle, ReleaseYear, Price FROM music WHERE Artist = ? AND SongTitle = ?",
			Parameters:  []interface{}{"Artist0", "Title0"},
			Expected: []*Music{
				{
					Artist:      "Artist0",
					SongTitle:   "Title0",
					ReleaseYear: 2000,
					Price:       0.5,
				},
			},
		},
		{
			Description: "Read records  with in operator",
			SQL:         "SELECT Artist, SongTitle, ReleaseYear, Price FROM music WHERE Artist IN(?, ?)",
			Parameters:  []interface{}{"Artist0", "Artist1"},
			Expected: []*Music{
				{
					Artist:      "Artist0",
					SongTitle:   "Title0",
					ReleaseYear: 2000,
					Price:       0.5,
				},
				{
					Artist:      "Artist1",
					SongTitle:   "Title1",
					ReleaseYear: 2001,
					Price:       1.5,
				},
			},
		},
		{
			Description: "Read records  with in operator",
			SQL:         "SELECT Artist, SongTitle, ReleaseYear, Price FROM music WHERE (Artist, SongTitle) IN ((?, ?), (?, ?))",
			Parameters:  []interface{}{"Artist0", "Title0", "Artist2", "Title2"},
			Expected: []*Music{
				{
					Artist:      "Artist0",
					SongTitle:   "Title0",
					ReleaseYear: 2000,
					Price:       0.5,
				},
				{
					Artist:      "Artist2",
					SongTitle:   "Title2",
					ReleaseYear: 2002,
					Price:       2.5,
				},
			},
		},
	}

	for _, useCase := range queryCases {
		var records = make([]*Music, 0)
		err = manager.ReadAll(&records, useCase.SQL, useCase.Parameters, nil)
		if !assert.Nil(t, err) {
			log.Printf("%v", err)
			return
		}
		var expected = make([]map[string]interface{}, 0)
		expected = append(expected, map[string]interface{}{
			"@indexBy@": "Artist,SongTitle",
		})
		for _, item := range useCase.Expected {
			var record = make(map[string]interface{})
			_ = toolbox.DefaultConverter.AssignConverted(&record, item)
		}
		assertly.AssertValues(t, expected, records, useCase.Description)
	}

	{ //Test persist

		var records = []*Music{

			{
				Artist:      "Artist0",
				SongTitle:   "Title0",
				ReleaseYear: 2010,
				Price:       0.5,
			},
			{
				Artist:      "Artist1",
				SongTitle:   "Title1",
				ReleaseYear: 2101,
				Price:       1.5,
			},
			{
				Artist:      "Artist5",
				SongTitle:   "Title5",
				ReleaseYear: 2005,
				Price:       5.5,
			},
		}

		inserted, updated, err := manager.PersistAll(&records, "music", nil)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, inserted)
		assert.EqualValues(t, 2, updated)
	}

}
