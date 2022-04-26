package graph

type vertex struct {
	name  string
	edges []*edge
}

type edge struct {
	to *vertex
}

func GetPaths(originFrom, originTo *vertex) ([][]*vertex, bool) {
	var getPaths func(from, to *vertex, firstRun bool) ([][]*vertex, bool)
	getPaths = func(from, to *vertex, firstRun bool) ([][]*vertex, bool) {
		res := [][]*vertex{}
		for i := 0; i < len(from.edges); i++ {
			if firstRun {
				firstRun = false
			} else if from == originFrom {
				continue
			}

			if from.edges[i].to == from {
				continue
			}

			if from.edges[i].to == originFrom {
				continue
			}

			if from.edges[i].to == originTo {
				res = append(res, []*vertex{originTo})
				continue
			}

			r, ok := getPaths(from.edges[i].to, to, false)
			if !ok {
				continue
			}

			for j := 0; j < len(r); j++ {
				rj := make([]*vertex, len(r[j])+1)
				rj[0] = from
				copy(rj[1:], r[j])
				r[j] = rj
			}

			res = append(res, r...)
		}

		return res, len(res) > 0
	}

	return getPaths(originFrom, originTo, true)
}
