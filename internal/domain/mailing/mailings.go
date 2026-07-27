package mailing

// Represents all persistent mailings
type Mailings interface {
	Mailing(ID) Mailing
}
