package configuration

// The Configuration sets the relationship between the organization and the smartcontract.
type Configuration struct {
	Organization string `json:"o"`
	Project      string `json:"p"`
	NetworkId    string `json:"n"`
	Group        string `json:"g"`
	Name         string `json:"s"`
	address      string
	id           uint64
	exists       bool
}

// Unique id of the configuration generated by the database
func (c *Configuration) SetId(id uint64) {
	c.exists = true
	c.id = id
}

// The smartcontract address to which the configuration belongs too.
func (c *Configuration) SetAddress(address string) {
	c.address = address
}

func (c *Configuration) Address() string {
	return c.address
}

func (c *Configuration) Id() uint64 {
	return c.id
}

func (c *Configuration) Exists() bool { return c.exists }
