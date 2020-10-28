const csv = require('fast-csv')
const fs = require('fs')
const path = require('path')

const filename = './ninja_europe_pv_v1.1/ninja_pv_europe_v1.1_merra2.csv'
// const filename = './ninja_europe_pv_v1.1/ninja_pv_europe_v1.1_sarah.csv'
// const filename = './ninja_europe_wind_v1.1/ninja_wind_europe_v1.1_future_nearterm_on-offshore.csv'
// const filename = './ninja_europe_wind_v1.1/ninja_wind_europe_v1.1_current_national.csv'
// const filename = './ninja_europe_wind_v1.1/ninja_wind_europe_v1.1_current_on-offshore.csv'
// const filename = './ninja_europe_wind_v1.1/ninja_wind_europe_v1.1_future_longterm_national.csv'
// const filename = './ninja_europe_wind_v1.1/ninja_wind_europe_v1.1_future_nearterm_national.csv'
const outputname = `output_${Date.now()}.csv`


// filtering functions
const parseDate = date => {
    date = date.split(' ')
    const [year, month, day] = date[0].split('-')
    const [hour, minute, second] = date[1].split(':')
    return { year, month, day, hour, minute, second }
}

const filterKeys = (data, keys) => data.map(d => {
    const o = {}
    keys.forEach(k => o[k] = d[k])
    return o
})

const filterDate = (data, date) => data.filter(d => d.time.startsWith(date))

const filterDates = (data, from, to) => {
    from = from || data[0].time
    to = to || data[data.length - 1].time
    if(from > to) [from, to] = [to, from]
    return data.filter(d => d.time >= from && d.time <= to)
}

const sumBy = (data, key) => {
    if(!['year', 'month', 'day'].includes(key)) return []

    const newData = []

    let curr, currTime
    const setCurrent = i => {
        curr = data[i]
        Object.keys(curr).filter(k => k !== 'time').forEach(k => curr[k] = parseFloat(curr[k]))
        currTime = parseDate(curr.time)
        curr.time = key === 'year' ? `${currTime.year}` :
            key === 'month' ? `${currTime.year}-${currTime.month}` :
            `${currTime.year}-${currTime.month}-${currTime.day}`
    }
    setCurrent(0)

    for(let i = 1; i < data.length; i++) {
        const d = {...data[i]}
        const t = parseDate(d.time)
        delete d.time
        if(currTime[key] === t[key]) {
            Object.keys(d).forEach(k => curr[k] += parseFloat(d[k]))
        }
        else {
            Object.keys(curr).filter(k => k !== 'time').forEach(k => curr[k] = curr[k].toFixed(6))
            newData.push(curr)
            setCurrent(i)
        }
    }
    Object.keys(curr).filter(k => k !== 'time').forEach(k => curr[k] = curr[k].toFixed(6))
    newData.push(curr)

    return newData
}

const averageBy = (data, key) => {
    if(!['year', 'month', 'day'].includes(key)) return []

    const newData = []

    let curr, currTime
    const setCurrent = i => {
        curr = data[i]
        Object.keys(curr).filter(k => k !== 'time').forEach(k => curr[k] = parseFloat(curr[k]))
        currTime = parseDate(curr.time)
        curr.time = key === 'year' ? `${currTime.year}` :
            key === 'month' ? `${currTime.year}-${currTime.month}` :
            `${currTime.year}-${currTime.month}-${currTime.day}`
    }
    setCurrent(0)

    let n = 0
    for(let i = 1; i < data.length; i++) {
        const d = {...data[i]}
        const t = parseDate(d.time)
        delete d.time
        if(currTime[key] === t[key]) {
            Object.keys(d).forEach(k => curr[k] += parseFloat(d[k]))
            n++
        }
        else {
            Object.keys(curr).filter(k => k !== 'time').forEach(k => curr[k] = (curr[k] / n).toFixed(6))
            newData.push(curr)
            n = 0
            setCurrent(i)
        }
    }
    Object.keys(curr).filter(k => k !== 'time').forEach(k => curr[k] = (curr[k] / n).toFixed(6))
    newData.push(curr)

    return newData
}


// filtering here
function reworkData(data) {
    console.log(data[0])

    // data = filterDate(data, '2005-09') // keep all entries where time starts with 2005-09
    data = filterDates(data, '2000-08-21 10', '2000-09') // limit to period 2000-08-21 10:00:00 to 2000-08-31 23:00:00
    data = filterKeys(data, ['time', 'FR']) // keep only time and FR keys
    // data = sumBy(data, 'month') // outputs sum for each keys by month
    data = averageBy(data, 'day') // outputs average for each keys by day
    
    // console.log(data)
    saveCSV(data, outputname)
}


// load CSV file + parse
const data = []
const stream = fs.createReadStream(path.resolve('./', `${filename}`))
    .pipe(csv.parse({ headers: true, delimiter: ',' }))
    .on('readable', () => {
        let row = null
        while((row = stream.read()) !== null) data.push(row)
    })
    .on('end', () => reworkData(data))


// save CSV
// require a 1D Array of flat objects
function saveCSV(array, filename) {
    const csv = []
    let headers = Object.keys(array[0])
    csv.push(headers.join(','))
    array.forEach(d => csv.push(headers.map(key => d[key]).join(',')))

    fs.writeFile(`./${filename}`, csv.join('\n'), error => {
        if(error) return console.log({ error })
        console.log('CSV file saved!')
        process.exit()
    })
}