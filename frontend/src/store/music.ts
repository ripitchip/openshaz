import { ref } from 'vue'

// Backend configuration
export const backendUrl = ref(localStorage.getItem('backend_url') || 'http://localhost:8000')
export const healthStatus = ref({ connected: false, message: '' })
export const isCheckingHealth = ref(false)

export const checkHealth = async () => {
    isCheckingHealth.value = true
    healthStatus.value = { connected: false, message: 'Checking...' }

    try {
        const response = await fetch(`${backendUrl.value}/health`)

        if (response.ok) {
            const data = await response.json()
            healthStatus.value = { connected: true, message: `Connected: ${data.status || 'OK'}` }
        } else {
            healthStatus.value = { connected: false, message: `Error: ${response.status}` }
        }
    } catch (err: any) {
        healthStatus.value = { connected: false, message: `Network error: ${err.message}` }
    } finally {
        isCheckingHealth.value = false
    }
}

export const saveBackendUrl = (url: string) => {
    backendUrl.value = url
    localStorage.setItem('backend_url', url)
    healthStatus.value = { connected: false, message: '' }
}

// Music data types
export interface Music {
    id: string
    title: string
    artist: string
    genre: string
    bpm: number
    key: string
}

// Music state
export const musicList = ref<Music[]>([])
export const selectedMusic = ref<string[]>([])

// Music functions
export const addMusic = (music: Omit<Music, 'id'>) => {
    const newMusic: Music = {
        ...music,
        id: Date.now().toString()
    }
    musicList.value.push(newMusic)
}

export const toggleSelection = (id: string) => {
    const index = selectedMusic.value.indexOf(id)
    if (index > -1) {
        selectedMusic.value.splice(index, 1)
    } else {
        if (selectedMusic.value.length < 2) {
            selectedMusic.value.push(id)
        } else {
            selectedMusic.value = [id]
        }
    }
}

export const getSelectedMusicDetails = () => {
    return selectedMusic.value
        .map(id => musicList.value.find(m => m.id === id))
        .filter((m): m is Music => m !== undefined)
}

export const calculateSimilarity = (music1: Music, music2: Music): number => {
    let score = 0

    // Genre match (40%)
    if (music1.genre.toLowerCase() === music2.genre.toLowerCase()) {
        score += 40
    }

    // BPM similarity (30%)
    const bpmDiff = Math.abs(music1.bpm - music2.bpm)
    const bpmScore = Math.max(0, 30 - (bpmDiff / 10) * 30)
    score += bpmScore

    // Key match (30%)
    if (music1.key.toLowerCase() === music2.key.toLowerCase()) {
        score += 30
    }

    return Math.round(score)
}

// Comparison history
export interface ComparisonEntry {
    id: string
    filename: string
    timestamp: number
    results: any[]
}

const storedHistory = localStorage.getItem('comparison_history')
export const comparisonHistory = ref<ComparisonEntry[]>(storedHistory ? JSON.parse(storedHistory) : [])

export const addComparisonHistory = (filename: string, results: any[]) => {
    const entry: ComparisonEntry = {
        id: crypto.randomUUID?.() || Date.now().toString(),
        filename,
        timestamp: Date.now(),
        results,
    }
    comparisonHistory.value = [entry, ...comparisonHistory.value].slice(0, 50)
    localStorage.setItem('comparison_history', JSON.stringify(comparisonHistory.value))
}