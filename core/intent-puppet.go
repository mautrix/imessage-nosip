package core

import (
	"fmt"
	"sync"

	log "maunium.net/go/maulogger/v2"

	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/id"
)

type IPuppet interface {
	GetMXID() id.UserID
	GetDisplayname() string
	GetAvatarURL() id.ContentURI
	GetIntent() *appservice.IntentAPI
}

type IntentPuppet struct {
	Intent *appservice.IntentAPI

	log         log.Logger
	writeLock   sync.Mutex
	displayName *string
	avatarURL   *id.ContentURI
}

func NewIntentPuppet(intent *appservice.IntentAPI) *IntentPuppet {
	return &IntentPuppet{
		Intent: intent,
		log:    log.Create().Sub(fmt.Sprintf("IntentPuppet/%s", intent.AppServiceUserID)),
	}
}

func (puppet *IntentPuppet) Refresh() {
	puppet.displayName = nil
	puppet.avatarURL = nil
	_ = puppet.GetDisplayname()
	_ = puppet.GetAvatarURL()
}

func (puppet *IntentPuppet) GetDisplayname() string {
	if puppet.displayName != nil {
		return *puppet.displayName
	}
	if resp, err := puppet.Intent.GetDisplayName(puppet.GetMXID()); err != nil {
		puppet.log.Errorfln("Failed to get display name: %w", err)
	} else if resp != nil {
		puppet.writeLock.Lock()
		puppet.displayName = &resp.DisplayName
		puppet.writeLock.Unlock()
		return resp.DisplayName
	}
	return ""
}

func (puppet *IntentPuppet) GetAvatarURL() id.ContentURI {
	if puppet.avatarURL != nil {
		return *puppet.avatarURL
	}
	if resp, err := puppet.Intent.GetAvatarURL(puppet.GetMXID()); err != nil {
		puppet.log.Errorfln("Failed to get display name: %w", err)
		return id.ContentURI{}
	} else {
		puppet.writeLock.Lock()
		puppet.avatarURL = &resp
		puppet.writeLock.Unlock()
		return resp
	}
}

func (puppet *IntentPuppet) GetMXID() id.UserID {
	return puppet.Intent.AppServiceUserID
}

func (puppet *IntentPuppet) GetIntent() *appservice.IntentAPI {
	return puppet.Intent
}
