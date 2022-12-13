// Handles the user's authentication
package account

import (
	"github.com/blocklords/gosds/env"
	"github.com/blocklords/gosds/message"
)

// Requeter to the SDS Service. It's either a developer or another SDS service.
type Account struct {
	id             uint64   // Auto incremented for every new developer
	PublicKey      string   // Public Key for authentication.
	Organization   string   // Organization
	NonceTimestamp uint64   // Nonce since the last usage. Only acceptable for developers
	service        *env.Env // If the account is another service, then this parameter keeps the data. Otherwise this parameter is a nil.
}

type Accounts []*Account

// Creates a new Account for a developer.
func NewDeveloper(id uint64, public_key string, nonce_timestamp uint64, organization string) *Account {
	return &Account{
		id:             id,
		PublicKey:      public_key,
		NonceTimestamp: nonce_timestamp,
		Organization:   organization,
		service:        nil,
	}
}

// Creates a new Account for a service
func NewService(service *env.Env) *Account {
	return &Account{
		id:             0,
		NonceTimestamp: 0,
		PublicKey:      service.PublicKey(),
		Organization:   "",
		service:        service,
	}
}

func (account *Account) IsDeveloper() bool {
	return account.service == nil
}

func (account *Account) IsService() bool {
	return account.service != nil
}

func (account *Account) ToJSON() map[string]interface{} {
	return map[string]interface{}{
		"id":              account.id,
		"nonce_timestamp": account.NonceTimestamp,
		"public_key":      account.PublicKey,
		"organization":    account.Organization,
	}
}

func ParseJson(raw map[string]interface{}) (*Account, error) {
	public_key, err := message.GetString(raw, "public_key")
	if err != nil {
		return nil, err
	}
	service, err := env.GetByPublicKey(public_key)
	if err != nil {
		id, err := message.GetUint64(raw, "id")
		if err != nil {
			return nil, err
		}
		nonce_timestamp, err := message.GetUint64(raw, "nonce_timestamp")
		if err != nil {
			return nil, err
		}

		organization, err := message.GetString(raw, "organization")
		if err != nil {
			return nil, err
		}
		return NewDeveloper(id, public_key, nonce_timestamp, organization), nil
	} else {
		return NewService(service), nil
	}
}

///////////////////////////////////////////////////////////
//
// Group operations
//
///////////////////////////////////////////////////////////

func NewAccounts(new_accounts ...*Account) Accounts {
	accounts := make(Accounts, len(new_accounts))
	for i, a := range new_accounts {
		accounts[i] = a
	}

	return accounts
}

func NewAccountsFromJson(raw_accounts []map[string]interface{}) (Accounts, error) {
	accounts := make(Accounts, len(raw_accounts))

	for _, raw := range raw_accounts {
		account, err := ParseJson(raw)
		if err != nil {
			return nil, err
		}

		accounts = accounts.Add(account)
	}

	return accounts, nil
}

func (accounts Accounts) Add(new_accounts ...*Account) Accounts {
	for _, account := range new_accounts {
		accounts = append(accounts, account)
	}

	return accounts
}

func (accounts Accounts) Remove(new_accounts ...*Account) Accounts {
	for _, account := range new_accounts {
		for i := range accounts {
			if account.PublicKey == accounts[i].PublicKey {
				accounts = append(accounts[:i], accounts[i+1:]...)
				return accounts
			}
		}
	}

	return accounts
}

func (accounts Accounts) PublicKeys() []string {
	public_keys := make([]string, len(accounts))

	for i := range accounts {
		public_keys[i] = accounts[i].PublicKey
	}

	return public_keys
}
