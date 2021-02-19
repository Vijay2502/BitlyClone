package main

type MyUrl struct {
	Id          int 	`json:"-"`
	Code   		string    	
	ShortUrl 	string	    
	LongUrl 	string
	Count		int	    
}

