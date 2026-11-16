package daemon

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"

	"github.com/mithrel-dots/tarragon/internal/plugins"
	"github.com/mithrel-dots/tarragon/internal/wire"
)

const actionOffersFeature = "action_offers"

func readResultRef(raw json.RawMessage) *wire.ResultRef {
	if len(raw) == 0 {
		return nil
	}
	var ref wire.ResultRef
	if json.Unmarshal(raw, &ref) != nil || !plugins.ReferenceKind(ref.Type) ||
		ref.Value == "" || strings.ContainsRune(ref.Value, 0) ||
		(ref.Type == "file" && !filepath.IsAbs(ref.Value)) {
		return nil
	}
	if _, ok := plugins.MediaType(ref.MIME, false); !ok {
		return nil
	}
	return &ref
}

var offerKey = []byte(rand.Text())

// A replacement with the same binding keeps its ID. Including the token in
// legacy names avoids retaining a tombstone for every old row of a live view.
func (q *queryState) makeOffer(row *wire.ResultItem, h plugins.Handler, capable bool) wire.Action {
	mac := hmac.New(sha256.New, offerKey)
	_ = json.NewEncoder(mac).Encode([]any{q.session, q.QueryID, row.Plugin, row.ID, row.Ref, h})
	id := base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:16])
	if capable {
		return wire.Action{Name: h.Label, Secondary: true, OfferID: id}
	}
	return wire.Action{Name: h.Label + " [via " + h.Plugin + "/" + h.Action + " #" + id + "]", Secondary: true}
}

// Only the daemon's generated suffix is reserved; ordinary action names
// remain selectable even when their original query has been evicted.
func legacyOfferName(name string) bool {
	if !strings.HasSuffix(name, "]") || !strings.Contains(name, " [via ") {
		return false
	}
	i := strings.LastIndex(name, " #")
	if i == -1 || len(name[i+2:len(name)-1]) != base64.RawURLEncoding.EncodedLen(16) {
		return false
	}
	_, err := base64.RawURLEncoding.DecodeString(name[i+2 : len(name)-1])
	return err == nil
}

func (q *queryState) addOffersLocked(rows []wire.ResultItem, mgr *plugins.Manager) {
	if mgr == nil || len(rows) == 0 {
		return
	}
	handlers := mgr.Handlers()
	if len(handlers) == 0 || !slices.ContainsFunc(rows, func(row wire.ResultItem) bool { return row.Ref != nil }) {
		return
	}
	rowCounts := make(map[string]int, len(rows))
	for _, row := range rows {
		rowCounts[row.ID]++
	}
	capable := slices.Contains(q.uiFeatures, actionOffersFeature)
	for i := range rows {
		row := &rows[i]
		if row.Ref == nil || rowCounts[row.ID] != 1 || !capable && len(row.Actions) == 0 {
			continue
		}
		names := make(map[string]int, len(row.Actions))
		for _, action := range row.Actions {
			names[action.Name]++
		}
		native := len(row.Actions)
		for _, h := range handlers {
			if h.Plugin != row.Plugin && h.Matches(row.Ref.Type, row.Ref.MIME) {
				offer := q.makeOffer(row, h, capable)
				names[offer.Name]++
				row.Actions = append(row.Actions, offer)
			}
		}
		offers := slices.DeleteFunc(row.Actions[native:], func(a wire.Action) bool { return names[a.Name] != 1 })
		row.Actions = row.Actions[:native+len(offers)]
	}
}

// Called only on the owner's current row, with q.mu held, before admission.
func (q *queryState) bindOfferLocked(a *selectAction, row *wire.ResultItem, selected wire.Action) bool {
	d := q.producers[row.Plugin]
	if d == nil || row.Ref == nil {
		return false
	}
	capable := slices.Contains(q.uiFeatures, actionOffersFeature)
	for _, h := range d.mgr.Handlers() {
		if h.Plugin == row.Plugin || !h.Matches(row.Ref.Type, row.Ref.MIME) {
			continue
		}
		offer := q.makeOffer(row, h, capable)
		if offer.Name == selected.Name && offer.OfferID == selected.OfferID {
			a.handler, a.action, a.handlerValue = h.Plugin, h.Action, row.Ref.Value
			a.handlerRule = &h
			a.pluginContext = ""
			return true
		}
	}
	return false
}

func (a *selectAction) handlerRow() (queryID, resultID string) {
	if a.handlerRule != nil {
		return "", a.handlerValue
	}
	return a.queryID, a.resultID
}
