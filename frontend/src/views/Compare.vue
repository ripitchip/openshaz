<script setup lang="ts">
import { ref } from 'vue'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { Input } from '@/components/ui/input'
import { backendUrl, addComparisonHistory, comparisonHistory } from '@/store/music'

const selectedFile = ref<File | null>(null)
const isAnalyzing = ref(false)
const errorMessage = ref('')
const similarSongs = ref<any[]>([])
const currentlyPlaying = ref<string | null>(null)
const audioElement = ref<HTMLAudioElement | null>(null)
const previewAudio = ref<HTMLAudioElement | null>(null)
const extractStart = ref<number>(0)
const extractDuration = ref<number>(30)
const isPlayingPreview = ref(false)
const currentPreviewTime = ref<number>(0)
const totalDuration = ref<number>(0)
const fileUrl = ref<string>('')
const waveformCanvas = ref<HTMLCanvasElement | null>(null)
const waveformData = ref<Uint8Array | null>(null)
const isDraggingStart = ref(false)
const isDraggingEnd = ref(false)
const isDraggingMiddle = ref(false)
const dragStartOffset = ref<number>(0)
const currentAudioBuffer = ref<AudioBuffer | null>(null)

const handleFileChange = (event: Event) => {
  const target = event.target as HTMLInputElement
  if (target.files && target.files.length > 0) {
    selectedFile.value = target.files[0] || null
    errorMessage.value = ''
    
    // Create URL for preview
    if (fileUrl.value) {
      URL.revokeObjectURL(fileUrl.value)
    }
    if (selectedFile.value) {
      fileUrl.value = URL.createObjectURL(selectedFile.value)
    }
    
    // Create audio element for preview
    const audio = new Audio(fileUrl.value)
    previewAudio.value = audio
    
    audio.onloadedmetadata = () => {
      totalDuration.value = audio.duration || 0
      
      // Decode audio for waveform
      const audioContext = new AudioContext()
      selectedFile.value?.arrayBuffer().then(buffer => {
        audioContext.decodeAudioData(buffer, (audioBuffer: AudioBuffer) => {
          drawWaveform(audioBuffer)
        })
      })
    }
    
    audio.ontimeupdate = () => {
      currentPreviewTime.value = (audio.currentTime as number) || 0
      if (waveformData.value) {
        drawWaveform(previewAudio.value as any)
      }
    }
    
    audio.onended = () => {
      isPlayingPreview.value = false
    }
    
    // Reset extract settings
    extractStart.value = 0
    extractDuration.value = 30
  }
}

const handleGetSimilar = async () => {
  if (!selectedFile.value) {
    errorMessage.value = 'Please select a file'
    return
  }

  isAnalyzing.value = true
  errorMessage.value = ''
  similarSongs.value = []

  try {
    // Extract the audio segment using Web Audio API
    const extractedBlob = await extractAudioSegment(selectedFile.value, extractStart.value, extractDuration.value)
    
    const formData = new FormData()
    // Add timestamp to filename to distinguish different segments
    const fileNameWithoutExt = selectedFile.value.name.replace(/\.[^/.]+$/, '')
    const fileExt = selectedFile.value.name.substring(selectedFile.value.name.lastIndexOf('.'))
    const timestampedName = `${fileNameWithoutExt} (${formatTime(extractStart.value)}-${formatTime(extractStart.value + extractDuration.value)})${fileExt}`
    
    formData.append('file', extractedBlob, timestampedName)
    // Also send segment info separately
    formData.append('segment_start', extractStart.value.toString())
    formData.append('segment_duration', extractDuration.value.toString())

    const response = await fetch(`${backendUrl.value}/get-similar`, {
      method: 'POST',
      body: formData
    })

    if (response.ok) {
      const data = await response.json()
      similarSongs.value = data.similar_songs || data.results || []
      addComparisonHistory(selectedFile.value.name, similarSongs.value)
    } else {
      errorMessage.value = `Error: ${response.status} ${response.statusText}`
    }
  } catch (err: any) {
    errorMessage.value = `Analysis failed: ${err.message}`
  } finally {
    isAnalyzing.value = false
  }
}

const extractAudioSegment = async (file: File, start: number, duration: number): Promise<Blob> => {
  const audioContext = new AudioContext()
  const arrayBuffer = await file.arrayBuffer()
  const audioBuffer = await audioContext.decodeAudioData(arrayBuffer)
  
  const sampleRate = audioBuffer.sampleRate
  const startSample = Math.floor(start * sampleRate)
  const durationSamples = Math.floor(duration * sampleRate)
  const endSample = Math.min(startSample + durationSamples, audioBuffer.length)
  
  const numberOfChannels = audioBuffer.numberOfChannels
  const extractedBuffer = audioContext.createBuffer(numberOfChannels, endSample - startSample, sampleRate)
  
  for (let channel = 0; channel < numberOfChannels; channel++) {
    const channelData = audioBuffer.getChannelData(channel)
    const extractedData = extractedBuffer.getChannelData(channel)
    for (let i = 0; i < extractedBuffer.length; i++) {
      extractedData[i] = channelData[startSample + i] ?? 0
    }
  }
  
  // Convert to WAV blob
  const wavBlob = audioBufferToWav(extractedBuffer)
  return wavBlob
}

const audioBufferToWav = (buffer: AudioBuffer): Blob => {
  const length = buffer.length * buffer.numberOfChannels * 2
  const arrayBuffer = new ArrayBuffer(44 + length)
  const view = new DataView(arrayBuffer)
  const channels: Float32Array[] = []
  let offset = 0
  let pos = 0
  
  // Write WAV header
  const setUint16 = (data: number) => {
    view.setUint16(pos, data, true)
    pos += 2
  }
  const setUint32 = (data: number) => {
    view.setUint32(pos, data, true)
    pos += 4
  }
  
  // "RIFF" chunk descriptor
  setUint32(0x46464952)
  setUint32(36 + length)
  setUint32(0x45564157)
  
  // "fmt " sub-chunk
  setUint32(0x20746d66)
  setUint32(16)
  setUint16(1)
  setUint16(buffer.numberOfChannels)
  setUint32(buffer.sampleRate)
  setUint32(buffer.sampleRate * buffer.numberOfChannels * 2)
  setUint16(buffer.numberOfChannels * 2)
  setUint16(16)
  
  // "data" sub-chunk
  setUint32(0x61746164)
  setUint32(length)
  
  // Write audio data
  for (let i = 0; i < buffer.numberOfChannels; i++) {
    channels.push(buffer.getChannelData(i))
  }
  
  while (pos < arrayBuffer.byteLength) {
    for (let i = 0; i < buffer.numberOfChannels; i++) {
      const channelData = channels[i]
      if (!channelData) continue
      let sample = Math.max(-1, Math.min(1, channelData[offset] ?? 0))
      sample = sample < 0 ? sample * 0x8000 : sample * 0x7FFF
      view.setInt16(pos, sample, true)
      pos += 2
    }
    offset++
  }
  
  return new Blob([arrayBuffer], { type: 'audio/wav' })
}

const togglePreview = () => {
  if (!previewAudio.value) return
  
  if (isPlayingPreview.value) {
    previewAudio.value.pause()
    isPlayingPreview.value = false
  } else {
    previewAudio.value.currentTime = 0
    previewAudio.value.play()
    isPlayingPreview.value = true
  }
}

const playExtractPreview = () => {
  if (!previewAudio.value) return
  
  previewAudio.value.currentTime = extractStart.value
  previewAudio.value.play()
  isPlayingPreview.value = true
  
  // Stop after duration
  setTimeout(() => {
    if (previewAudio.value && isPlayingPreview.value) {
      previewAudio.value.pause()
      isPlayingPreview.value = false
    }
  }, extractDuration.value * 1000)
}

const setStartFromCurrent = () => {
  if (previewAudio.value) {
    extractStart.value = Math.floor(previewAudio.value.currentTime ?? 0)
  }
}

const formatTime = (seconds: number): string => {
  const mins = Math.floor(seconds / 60)
  const secs = Math.floor(seconds % 60)
  return `${mins}:${secs.toString().padStart(2, '0')}`
}

const drawWaveform = (audioBuffer: AudioBuffer) => {
  if (!waveformCanvas.value) return
  
  currentAudioBuffer.value = audioBuffer
  const canvas = waveformCanvas.value
  const ctx = canvas.getContext('2d')
  if (!ctx) return
  
  // Get audio data
  const rawData = audioBuffer.getChannelData(0)
  const samples = Math.floor(rawData.length / 100)
  const blockSize = Math.floor(rawData.length / samples)
  const filtered = new Uint8Array(samples)
  
  for (let i = 0; i < samples; i++) {
    let sum = 0
    for (let j = 0; j < blockSize; j++) {
      sum += Math.abs(rawData[i * blockSize + j] ?? 0)
    }
    filtered[i] = (sum / blockSize) * 256
  }
  
  waveformData.value = filtered
  
  // Draw waveform
  const canvasWidth = canvas.offsetWidth || 400
  const canvasHeight = canvas.offsetHeight || 128
  canvas.width = canvasWidth
  canvas.height = canvasHeight
  
  const barWidth = (canvasWidth / samples)
  const centerY = canvasHeight / 2
  
  // Draw background
  ctx.fillStyle = '#0f172a'
  ctx.fillRect(0, 0, canvasWidth, canvasHeight)
  
  // Draw waveform
  ctx.fillStyle = '#06b6d4'
  for (let i = 0; i < samples; i++) {
    const barHeight = ((filtered[i] ?? 0) / 255) * centerY
    ctx.fillRect(i * barWidth, centerY - barHeight, barWidth - 1, barHeight * 2)
  }
  
  // Draw playback position
  const playPos = ((currentPreviewTime.value ?? 0) / (totalDuration.value ?? 1)) * canvasWidth
  ctx.strokeStyle = '#0ea5e9'
  ctx.lineWidth = 2
  ctx.beginPath()
  ctx.moveTo(playPos, 0)
  ctx.lineTo(playPos, canvasHeight)
  ctx.stroke()
  
  // Draw extract range
  const startX = ((extractStart.value ?? 0) / (totalDuration.value ?? 1)) * canvasWidth
  const endX = (((extractStart.value ?? 0) + (extractDuration.value ?? 30)) / (totalDuration.value ?? 1)) * canvasWidth
  
  ctx.fillStyle = 'rgba(34, 197, 94, 0.2)'
  ctx.fillRect(startX, 0, endX - startX, canvasHeight)
  
  // Draw markers
  ctx.strokeStyle = '#22c55e'
  ctx.lineWidth = 3
  ctx.beginPath()
  ctx.moveTo(startX, 0)
  ctx.lineTo(startX, canvasHeight)
  ctx.stroke()
  
  ctx.strokeStyle = '#ef4444'
  ctx.beginPath()
  ctx.moveTo(endX, 0)
  ctx.lineTo(endX, canvasHeight)
  ctx.stroke()
}

const handleWaveformMouseDown = (e: MouseEvent) => {
  if (!waveformCanvas.value) return
  
  const canvas = waveformCanvas.value
  const rect = canvas.getBoundingClientRect()
  const x = e.clientX - rect.left
  const canvasWidth = canvas.width || 400
  
  const startX = ((extractStart.value ?? 0) / (totalDuration.value ?? 1)) * canvasWidth
  const endX = (((extractStart.value ?? 0) + (extractDuration.value ?? 30)) / (totalDuration.value ?? 1)) * canvasWidth
  
  // Check if clicking near left marker
  if (Math.abs(x - startX) < 15) {
    isDraggingStart.value = true
    e.preventDefault()
  } 
  // Check if clicking near right marker
  else if (Math.abs(x - endX) < 15) {
    isDraggingEnd.value = true
    e.preventDefault()
  }
  // Check if clicking in the middle of the selection block
  else if (x > startX && x < endX) {
    isDraggingMiddle.value = true
    dragStartOffset.value = x - startX
    e.preventDefault()
  }
}

const handleWaveformMouseMove = (e: MouseEvent) => {
  if (!waveformCanvas.value || totalDuration.value === 0) return
  
  const canvas = waveformCanvas.value
  const rect = canvas.getBoundingClientRect()
  const x = e.clientX - rect.left
  const canvasWidth = canvas.width || 400
  const startX = ((extractStart.value ?? 0) / (totalDuration.value ?? 1)) * canvasWidth
  const endX = (((extractStart.value ?? 0) + (extractDuration.value ?? 30)) / (totalDuration.value ?? 1)) * canvasWidth
  
  // Update cursor based on what's under the mouse
  if (Math.abs(x - startX) < 15 || Math.abs(x - endX) < 15) {
    canvas.style.cursor = 'col-resize'
  } else if (x > startX && x < endX) {
    canvas.style.cursor = 'grab'
  } else {
    canvas.style.cursor = 'pointer'
  }
  
  if (!isDraggingStart.value && !isDraggingEnd.value && !isDraggingMiddle.value) return
  
  const percent = Math.max(0, Math.min(1, x / canvasWidth))
  const time = percent * (totalDuration.value ?? 0)
  
  if (isDraggingStart.value) {
    // Expand/reduce from left - don't go past end
    const newStart = Math.max(0, Math.min(time, extractStart.value + (extractDuration.value ?? 30) - 5))
    const timeDiff = newStart - (extractStart.value ?? 0)
    extractStart.value = newStart
    extractDuration.value = Math.max(5, (extractDuration.value ?? 30) - timeDiff)
  } else if (isDraggingEnd.value) {
    // Expand/reduce from right - don't go past start
    const newEnd = Math.max((extractStart.value ?? 0) + 5, time)
    const newDuration = newEnd - (extractStart.value ?? 0)
    extractDuration.value = Math.min(newDuration, (totalDuration.value ?? 0) - (extractStart.value ?? 0))
  } else if (isDraggingMiddle.value) {
    // Move the entire selection block
    const newStart = time - (dragStartOffset.value / canvasWidth) * (totalDuration.value ?? 0)
    const minStart = 0
    const maxStart = (totalDuration.value ?? 0) - (extractDuration.value ?? 30)
    extractStart.value = Math.max(minStart, Math.min(newStart, maxStart))
  }
  
  // Redraw waveform with updated markers
  if (currentAudioBuffer.value) {
    drawWaveform(currentAudioBuffer.value)
  }
  
  e.preventDefault()
}

const handleWaveformMouseUp = () => {
  isDraggingStart.value = false
  isDraggingEnd.value = false
  isDraggingMiddle.value = false
}

const playAudio = (filename: string) => {
  if (currentlyPlaying.value === filename) {
    // Pause if already playing
    audioElement.value?.pause()
    currentlyPlaying.value = null
  } else {
    // Stop current audio if any
    if (audioElement.value) {
      audioElement.value.pause()
    }
    
    // Create new audio element
    const audio = new Audio(`${backendUrl.value}/get-song/${encodeURIComponent(filename)}`)
    audioElement.value = audio
    currentlyPlaying.value = filename
    
    audio.play().catch(err => {
      console.error('Failed to play audio:', err)
      errorMessage.value = 'Failed to play audio'
      currentlyPlaying.value = null
    })
    
    audio.onended = () => {
      currentlyPlaying.value = null
    }
  }
}

const downloadAudio = async (filename: string) => {
  try {
    // Fetch the audio file as a blob
    const response = await fetch(`${backendUrl.value}/get-song/${encodeURIComponent(filename)}`)
    if (!response.ok) {
      errorMessage.value = 'Failed to download audio file'
      return
    }
    
    const blob = await response.blob()
    
    // Create a blob URL and trigger download
    const blobUrl = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = blobUrl
    link.download = filename || 'audio.mp3'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    
    // Clean up the blob URL
    URL.revokeObjectURL(blobUrl)
  } catch (err: any) {
    console.error('Download failed:', err)
    errorMessage.value = `Download failed: ${err.message}`
  }
}
</script>

<template>
  <div class="max-w-5xl mx-auto">
    <div class="text-center mb-8">
      <h2 class="text-3xl font-bold text-blue-900 mb-2">Find Similar Tracks</h2>
      <p class="text-blue-700">Upload an audio file to find similar songs</p>
    </div>

    <Card class="bg-white shadow-2xl mb-8">
      <CardHeader>
        <CardTitle class="text-2xl text-blue-900">Upload Audio File</CardTitle>
        <CardDescription class="text-gray-600">
          Select an audio file to analyze and find similar tracks
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <label class="text-sm font-semibold text-gray-700">Audio File</label>
          <Input 
            type="file"
            @change="handleFileChange"
            accept="audio/*"
            class="h-12 text-lg cursor-pointer"
          />
          <p v-if="selectedFile" class="text-sm text-blue-600">
            Selected: {{ selectedFile.name }}
          </p>
        </div>

        <div v-if="selectedFile" class="space-y-4 p-4 bg-blue-50 rounded-lg">
          <div class="flex items-center justify-between">
            <span class="text-sm font-semibold text-gray-700">Preview Full Audio</span>
            <span class="text-sm text-gray-600">{{ formatTime(currentPreviewTime) }} / {{ formatTime(totalDuration) }}</span>
          </div>
          <div class="flex gap-2">
            <Button 
              @click="togglePreview"
              class="bg-white! text-blue-600! hover:bg-blue-100! border border-blue-200"
              size="sm"
            >
              {{ isPlayingPreview ? '⏸ Pause' : '▶ Play' }}
            </Button>
            <Button 
              @click="setStartFromCurrent"
              class="bg-white! text-blue-600! hover:bg-blue-100! border border-blue-200"
              size="sm"
            >
              Set Start Here
            </Button>
          </div>

          <Separator class="bg-blue-200" />

          <div class="space-y-3">
            <h4 class="text-sm font-semibold text-gray-700">Select Extract Segment</h4>
            <p class="text-xs text-gray-600">Drag the green (start) and red (end) markers to select your extract</p>
            <canvas 
              ref="waveformCanvas"
              @mousedown="handleWaveformMouseDown"
              @mousemove="handleWaveformMouseMove"
              @mouseup="handleWaveformMouseUp"
              @mouseleave="handleWaveformMouseUp"
              class="w-full h-32 bg-slate-900 rounded cursor-pointer border-2 border-blue-400 hover:border-blue-500 transition-colors"
              style="touch-action: none; user-select: none;"
            />
            <div class="flex justify-between text-xs text-gray-600">
              <span>Start: {{ formatTime(extractStart) }}</span>
              <span>Duration: {{ formatTime(extractDuration) }}</span>
              <span>End: {{ formatTime(extractStart + extractDuration) }}</span>
            </div>
            <Button 
              @click="playExtractPreview"
              class="w-full bg-blue-600! text-white! hover:bg-blue-700!"
              size="sm"
            >
              🎧 Preview Extract
            </Button>
          </div>
        </div>

        <div v-if="errorMessage" class="p-4 bg-red-50 border border-red-200 rounded-lg">
          <p class="text-red-600">{{ errorMessage }}</p>
        </div>
        <Button 
          @click="handleGetSimilar" 
          :disabled="!selectedFile || isAnalyzing"
          class="w-full h-14 text-lg bg-linear-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 text-white disabled:opacity-50"
        >
          <span class="mr-2">{{ isAnalyzing ? '⏳' : '🔍' }}</span> 
          {{ isAnalyzing ? 'Analyzing...' : 'Find Similar Tracks' }}
        </Button>
      </CardContent>
    </Card>

    <div v-if="similarSongs.length > 0">
      <Separator class="my-8 bg-gray-200" />
      
      <h3 class="text-2xl font-bold text-blue-900 mb-6">Similar Tracks Found</h3>
      
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <Card 
          v-for="(song, index) in similarSongs" 
          :key="index"
          class="bg-white shadow-xl"
        >
          <div class="aspect-square bg-linear-to-br from-blue-400 to-purple-500 flex items-center justify-center">
            <span class="text-6xl">🎵</span>
          </div>
          <CardHeader>
            <CardTitle class="text-lg text-gray-900">{{ song.title || song.name || 'Unknown' }}</CardTitle>
            <CardDescription class="text-gray-600">{{ song.artist || 'Unknown Artist' }}</CardDescription>
          </CardHeader>
          <CardContent>
            <div class="space-y-2">
              <div v-if="song.similarity" class="flex justify-between items-center">
                <span class="text-sm font-semibold">Similarity</span>
                <Badge class="bg-blue-600">{{ Math.round(song.similarity * 100) }}%</Badge>
              </div>
              <div v-if="song.genre" class="flex justify-between items-center">
                <span class="text-sm font-semibold">Genre</span>
                <Badge variant="secondary">{{ song.genre }}</Badge>
              </div>
              <div class="flex gap-2 mt-4">
                <Button 
                  @click="playAudio(song.name || song.title || song.filename)"
                  class="flex-1"
                  :class="currentlyPlaying === (song.name || song.title || song.filename) ? 'bg-linear-to-br! from-blue-400! to-blue-600! text-white! font-semibold' : 'bg-transparent! text-black! hover:bg-gray-100!'"
                  size="sm"
                >
                  <span class="mr-1">{{ currentlyPlaying === (song.name || song.title || song.filename) ? '⏸' : '▶' }}</span>
                  {{ currentlyPlaying === (song.name || song.title || song.filename) ? 'Pause' : 'Play' }}
                </Button>
                <Button 
                  @click="downloadAudio(song.name || song.title || song.filename)"
                  class="w-10 h-10 p-0 rounded-full bg-transparent! border-2! border-blue-600! text-blue-600! hover:bg-blue-50! flex items-center justify-center"
                  size="sm"
                  title="Download"
                >
                  <span>⬇</span>
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>

    <div v-else-if="!isAnalyzing && selectedFile" class="text-center py-12">
      <div class="text-6xl mb-4">🎧</div>
      <p class="text-blue-900 text-xl">Click "Find Similar Tracks" to analyze</p>
    </div>

    <div class="mt-12" v-if="comparisonHistory.length">
      <Separator class="my-8 bg-gray-200" />
      <h3 class="text-2xl font-bold text-blue-900 mb-6">Previous Comparisons</h3>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card v-for="entry in comparisonHistory" :key="entry.id" class="bg-white shadow-md">
          <CardHeader>
            <CardTitle class="text-lg text-gray-900">{{ entry.filename || 'Unknown file' }}</CardTitle>
            <CardDescription>{{ new Date(entry.timestamp).toLocaleString() }}</CardDescription>
          </CardHeader>
          <CardContent>
            <div class="space-y-2">
              <div v-for="(song, idx) in entry.results" :key="idx" class="space-y-2 border-b last:border-b-0 pb-3 last:pb-0">
                <div class="flex items-center justify-between">
                  <div>
                    <p class="text-sm font-semibold text-gray-900">{{ song.title || song.name || 'Unknown' }}</p>
                    <p class="text-xs text-gray-600">{{ song.artist || 'Unknown Artist' }}</p>
                  </div>
                  <Badge v-if="song.similarity" class="bg-blue-600">{{ Math.round(song.similarity * 100) }}%</Badge>
                </div>
                <div class="flex gap-2">
                  <Button 
                    @click="playAudio(song.name || song.title || song.filename)"
                    class="flex-1"
                    :class="currentlyPlaying === (song.name || song.title || song.filename) ? 'bg-linear-to-br! from-blue-400! to-blue-600! text-white! font-semibold' : 'bg-transparent! text-black! hover:bg-gray-100!'"
                    size="sm"
                  >
                    <span class="mr-1">{{ currentlyPlaying === (song.name || song.title || song.filename) ? '⏸' : '▶' }}</span>
                    {{ currentlyPlaying === (song.name || song.title || song.filename) ? 'Pause' : 'Play' }}
                  </Button>
                  <Button 
                    @click="downloadAudio(song.name || song.title || song.filename)"
                    class="w-10 h-10 p-0 rounded-full bg-transparent! border-2! border-blue-600! text-blue-600! hover:bg-blue-50! flex items-center justify-center"
                    size="sm"
                    title="Download"
                  >
                    <span>⬇</span>
                  </Button>
                </div>
              </div>
              <p v-if="!entry.results || entry.results.length === 0" class="text-sm text-gray-500">No results stored.</p>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  </div>
</template>
