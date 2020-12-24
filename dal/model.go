package dal

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// DbUser model utilisateur, table USER
type DbUser struct {
	ID        int       `json:"id"`
	Name      string    `json:"name" apiuse:"search,sort" dbfield:"USER.name"`
	Login     string    `json:"login" apiuse:"search,sort" dbfield:"USER.login"`
	Password  string    `json:"password,omitempty"`
	Activ     bool      `json:"activ" apiuse:"search,sort" dbfield:"USER.deleted_at"` /// todo pour filtrage des non actif ?
	DeletedAt time.Time `json:"-" apiuse:"search,sort" dbfield:"USER.deleted_at"`     /// todo pour filtrage des non actif ?
}

// ValidatePassword pour controle de validité
func (c *DbUser) ValidatePassword() error {
	if c.Password != strings.TrimSpace(c.Password) {
		return fmt.Errorf("leading or trailing white spaces forbidden")
	}
	if len(c.Password) < 8 {
		return fmt.Errorf("password len is < 9")
	}
	num := `[0-9]{1}`
	az := `[a-z]{1}`
	AZ := `[A-Z]{1}`
	symbol := `[!@#~$%^&*()+|_]{1}`
	if b, err := regexp.MatchString(num, c.Password); !b || err != nil {
		return fmt.Errorf("password need num :%v", err)
	}
	if b, err := regexp.MatchString(az, c.Password); !b || err != nil {
		return fmt.Errorf("password need a_z :%v", err)
	}
	if b, err := regexp.MatchString(AZ, c.Password); !b || err != nil {
		return fmt.Errorf("password need A_Z :%v", err)
	}
	if b, err := regexp.MatchString(symbol, c.Password); !b || err != nil {
		return fmt.Errorf("password need symbol :%v", err)
	}
	return nil
}

// Validate pour controle de validité
func (c *DbUser) Validate(Create bool) error {
	if Create && c.ID > 0 {
		return fmt.Errorf("invalid create")
	}

	//create ou maj password
	if Create || c.Password != "" {
		err := c.ValidatePassword()
		if err != nil {
			return err
		}
	}

	// autre spec au mode create
	if Create {
		cleanLogin := strings.ToLower(c.Login)
		cleanLogin = strings.ReplaceAll(cleanLogin, " ", "")
		if !strings.EqualFold(cleanLogin, c.Login) || len(cleanLogin) < 3 {
			return fmt.Errorf("invalid login")
		}
		c.Login = cleanLogin
	}

	c.Name = strings.TrimSpace(c.Name)
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("invalid name")
	}
	return nil
}
