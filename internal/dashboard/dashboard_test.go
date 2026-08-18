package dashboard

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestShouldStart(t *testing.T) {
	tests := []struct {
		workerID string
		want     bool
	}{{"download_host@1", true}, {"download_host@2", false}, {"download_host@12", false}, {"download_manual", true}}
	for _, test := range tests {
		if got := ShouldStart(test.workerID); got != test.want {
			t.Errorf("ShouldStart(%q) = %v, want %v", test.workerID, got, test.want)
		}
	}
}

func TestTimelineSteps(t *testing.T) {
	timeline := bson.D{{Key: "upload", Value: bson.D{{Key: "status", Value: "pending"}, {Key: "percent", Value: int32(0)}}}, {Key: "download", Value: bson.D{{Key: "status", Value: "completed"}, {Key: "percent", Value: float64(97)}}}, {Key: "merge", Value: bson.D{{Key: "status", Value: "processing"}, {Key: "percent", Value: int32(55)}}}}
	steps := timelineSteps(timeline)
	if len(steps) != 3 {
		t.Fatalf("got %d steps, want 3", len(steps))
	}
	if steps[0].Key != "download" || steps[0].Percent != 100 {
		t.Fatalf("first step = %#v", steps[0])
	}
	if steps[1].Key != "merge" || steps[1].Percent != 55 {
		t.Fatalf("second step = %#v", steps[1])
	}
}

func TestParseCodecUtilization(t *testing.T) {
	got := parseCodecUtilization("# gpu sm mem enc dec\n0 12 4 67 3\n1 8 2 - -")
	if got[0].encoder != 67 || got[0].decoder != 3 {
		t.Fatalf("GPU 0 = %#v", got[0])
	}
	if got[1].encoder != 0 || got[1].decoder != 0 {
		t.Fatalf("GPU 1 = %#v", got[1])
	}
}
